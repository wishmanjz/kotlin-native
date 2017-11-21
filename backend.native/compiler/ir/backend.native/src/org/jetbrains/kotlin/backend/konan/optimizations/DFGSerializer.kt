package org.jetbrains.kotlin.backend.konan.optimizations

import org.jetbrains.kotlin.backend.konan.Context
import org.jetbrains.kotlin.backend.konan.ModuleDataFlowGraph
import org.jetbrains.kotlin.config.CommonConfigurationKeys

internal class ExternalModulesDFG(val allTypes: List<DataFlowIR.Type.Declared>,
                                  val publicTypes: Map<String, DataFlowIR.Type.Public>,
                                  val publicFunctions: Map<String, DataFlowIR.FunctionSymbol.Public>,
                                  val functionDFGs: Map<DataFlowIR.FunctionSymbol, DataFlowIR.Function>)

internal object DFGSerializer {

    private val DEBUG = 0

    private inline fun DEBUG_OUTPUT(severity: Int, block: () -> Unit) {
        if (DEBUG > severity) block()
    }

    fun serialize(context: Context, moduleDFG: ModuleDFG) {
        val moduleDFGBuilder = context.dataFlowGraph.value
        val symbolTableBuilder = ModuleDataFlowGraph.SymbolTable.newBuilder()
        val symbolTable = moduleDFG.symbolTable
        val typeMap = (symbolTable.classMap.values + DataFlowIR.Type.Virtual).distinct().withIndex().associateBy({ it.value }, { it.index })
        val functionSymbolMap = symbolTable.functionMap.values.distinct().withIndex().associateBy({ it.value }, { it.index })
        DEBUG_OUTPUT(1) {
            println("TYPES: ${typeMap.size}, " +
                    "FUNCTIONS: ${functionSymbolMap.size}, " +
                    "PRIVATE FUNCTIONS: ${functionSymbolMap.keys.count { it is DataFlowIR.FunctionSymbol.Private }}, " +
                    "FUNCTION TABLE SIZE: ${symbolTable.couldBeCalledVirtuallyIndex}"
            )
        }
        for (type in typeMap.entries.sortedBy { it.value }.map { it.key }) {

            fun buildTypeIntestines(type: DataFlowIR.Type.Declared): ModuleDataFlowGraph.DeclaredType.Builder {
                val result = ModuleDataFlowGraph.DeclaredType.newBuilder()
                result.isFinal = type.isFinal
                result.isAbstract = type.isAbstract
                result.addAllSuperTypes(type.superTypes.map { typeMap[it]!! })
                result.addAllVtable(type.vtable.map { functionSymbolMap[it]!! })
                type.itable.forEach { hash, functionSymbol ->
                    val itableSlotBuilder = ModuleDataFlowGraph.ItableSlot.newBuilder()
                    itableSlotBuilder.hash = hash
                    itableSlotBuilder.impl = functionSymbolMap[functionSymbol]!!
                    result.addItable(itableSlotBuilder)
                }
                return result
            }

            val typeBuilder = ModuleDataFlowGraph.Type.newBuilder()
            when (type) {
                DataFlowIR.Type.Virtual -> {
                    typeBuilder.virtual = 1
                }

                is DataFlowIR.Type.External -> {
                    val externalTypeBuilder = ModuleDataFlowGraph.ExternalType.newBuilder()
                    externalTypeBuilder.name = type.name
                    typeBuilder.setExternal(externalTypeBuilder)
                }

                is DataFlowIR.Type.Public -> {
                    val publicTypeBuilder = ModuleDataFlowGraph.PublicType.newBuilder()
                    publicTypeBuilder.name = type.name
                    publicTypeBuilder.setIntestines(buildTypeIntestines(type))
                    typeBuilder.setPublic(publicTypeBuilder)
                }

                is DataFlowIR.Type.Private -> {
                    val privateTypeBuilder = ModuleDataFlowGraph.PrivateType.newBuilder()
                    privateTypeBuilder.name = type.name
                    privateTypeBuilder.index = type.index
                    privateTypeBuilder.setIntestines(buildTypeIntestines(type))
                    typeBuilder.setPrivate(privateTypeBuilder)
                }
            }
            symbolTableBuilder.addTypes(typeBuilder)
        }
        for (functionSymbol in functionSymbolMap.entries.sortedBy { it.value }.map { it.key }) {
            val functionSymbolBuilder = ModuleDataFlowGraph.FunctionSymbol.newBuilder()
            when (functionSymbol) {
                is DataFlowIR.FunctionSymbol.External -> {
                    val externalFunctionSymbolBuilder = ModuleDataFlowGraph.ExternalFunctionSymbol.newBuilder()
                    externalFunctionSymbolBuilder.name = functionSymbol.name
                    functionSymbolBuilder.setExternal(externalFunctionSymbolBuilder)
                }

                is DataFlowIR.FunctionSymbol.Public -> {
                    val publicFunctionSymbolBuilder = ModuleDataFlowGraph.PublicFunctionSymbol.newBuilder()
                    publicFunctionSymbolBuilder.name = functionSymbol.name
                    publicFunctionSymbolBuilder.index = functionSymbol.symbolTableIndex
                    functionSymbolBuilder.setPublic(publicFunctionSymbolBuilder)
                }

                is DataFlowIR.FunctionSymbol.Private -> {
                    val privateFunctionSymbolBuilder = ModuleDataFlowGraph.PrivateFunctionSymbol.newBuilder()
                    privateFunctionSymbolBuilder.name = functionSymbol.name
                    privateFunctionSymbolBuilder.index = functionSymbol.symbolTableIndex
                    functionSymbolBuilder.setPrivate(privateFunctionSymbolBuilder)
                }
            }
            symbolTableBuilder.addFunctionSymbols(functionSymbolBuilder)
        }
        moduleDFGBuilder.setSymbolTable(symbolTableBuilder)
        for (function in moduleDFG.functions.values) {
            val functionTemplateBuilder = ModuleDataFlowGraph.Function.newBuilder()
            functionTemplateBuilder.symbol = functionSymbolMap[function.symbol]!!
            functionTemplateBuilder.isGlobalInitializer = function.isGlobalInitializer
            functionTemplateBuilder.numberOfParameters = function.numberOfParameters
            val bodyBuilder = ModuleDataFlowGraph.FunctionBody.newBuilder()
            val body = function.body
            val nodeMap = body.nodes.withIndex().associateBy({ it.value }, { it.index })

            fun buildEdge(edge: DataFlowIR.Edge): ModuleDataFlowGraph.Edge.Builder {
                val result = ModuleDataFlowGraph.Edge.newBuilder()
                result.node = nodeMap[edge.node]!!
                edge.castToType?.let { result.castToType = typeMap[it]!! }
                return result
            }

            fun buildCall(call: DataFlowIR.Node.Call): ModuleDataFlowGraph.Call.Builder {
                val result = ModuleDataFlowGraph.Call.newBuilder()
                result.callee = functionSymbolMap[call.callee]!!
                result.returnType = typeMap[call.returnType]!!
                call.arguments.forEach {
                    result.addArguments(buildEdge(it))
                }
                return result
            }

            fun buildVirtualCall(virtualCall: DataFlowIR.Node.VirtualCall): ModuleDataFlowGraph.VirtualCall.Builder {
                val result = ModuleDataFlowGraph.VirtualCall.newBuilder()
                result.setCall(buildCall(virtualCall))
                result.receiverType = typeMap[virtualCall.receiverType]!!
                return result
            }

            fun buildField(field: DataFlowIR.Field): ModuleDataFlowGraph.Field.Builder {
                val result = ModuleDataFlowGraph.Field.newBuilder()
                field.type?.let { result.type = typeMap[it]!! }
                result.name = field.name
                return result
            }

            for (node in nodeMap.entries.sortedBy { it.value }.map { it.key }) {
                val nodeBuilder = ModuleDataFlowGraph.Node.newBuilder()
                when (node) {
                    is DataFlowIR.Node.Parameter -> {
                        val parameterBuilder = ModuleDataFlowGraph.Parameter.newBuilder()
                        parameterBuilder.index = node.index
                        nodeBuilder.setParameter(parameterBuilder)
                    }

                    is DataFlowIR.Node.Const -> {
                        val constBuilder = ModuleDataFlowGraph.Const.newBuilder()
                        constBuilder.type = typeMap[node.type]!!
                        nodeBuilder.setConst(constBuilder)
                    }

                    is DataFlowIR.Node.StaticCall -> {
                        val staticCallBuilder = ModuleDataFlowGraph.StaticCall.newBuilder()
                        staticCallBuilder.setCall(buildCall(node))
                        if (node.receiverType != null)
                            staticCallBuilder.receiverType = typeMap[node.receiverType]!!
                        nodeBuilder.setStaticCall(staticCallBuilder)
                    }

                    is DataFlowIR.Node.NewObject -> {
                        val newObjectBuilder = ModuleDataFlowGraph.NewObject.newBuilder()
                        newObjectBuilder.setCall(buildCall(node))
                        nodeBuilder.setNewObject(newObjectBuilder)
                    }

                    is DataFlowIR.Node.VtableCall -> {
                        val vTableCallBuilder = ModuleDataFlowGraph.VtableCall.newBuilder()
                        vTableCallBuilder.setVirtualCall(buildVirtualCall(node))
                        vTableCallBuilder.calleeVtableIndex = node.calleeVtableIndex
                        nodeBuilder.setVtableCall(vTableCallBuilder)
                    }

                    is DataFlowIR.Node.ItableCall -> {
                        val iTableCallBuilder = ModuleDataFlowGraph.ItableCall.newBuilder()
                        iTableCallBuilder.setVirtualCall(buildVirtualCall(node))
                        iTableCallBuilder.calleeHash = node.calleeHash
                        nodeBuilder.setItableCall(iTableCallBuilder)
                    }

                    is DataFlowIR.Node.Singleton -> {
                        val singletonBuilder = ModuleDataFlowGraph.Singleton.newBuilder()
                        singletonBuilder.type = typeMap[node.type]!!
                        node.constructor?.let { singletonBuilder.constructor = functionSymbolMap[it]!! }
                        nodeBuilder.setSingleton(singletonBuilder)
                    }

                    is DataFlowIR.Node.FieldRead -> {
                        val fieldReadBuilder = ModuleDataFlowGraph.FieldRead.newBuilder()
                        node.receiver?.let { fieldReadBuilder.setReceiver(buildEdge(it)) }
                        fieldReadBuilder.setField(buildField(node.field))
                        nodeBuilder.setFieldRead(fieldReadBuilder)
                    }

                    is DataFlowIR.Node.FieldWrite -> {
                        val fieldWriteBuilder = ModuleDataFlowGraph.FieldWrite.newBuilder()
                        node.receiver?.let { fieldWriteBuilder.setReceiver(buildEdge(it)) }
                        fieldWriteBuilder.setField(buildField(node.field))
                        fieldWriteBuilder.setValue(buildEdge(node.value))
                        nodeBuilder.setFieldWrite(fieldWriteBuilder)
                    }

                    is DataFlowIR.Node.Variable -> {
                        val variableBuilder = ModuleDataFlowGraph.Variable.newBuilder()
                        node.values.forEach {
                            variableBuilder.addValues(buildEdge(it))
                        }
                        nodeBuilder.setVariable(variableBuilder)
                    }

                    is DataFlowIR.Node.TempVariable -> {
                        val tempVariableBuilder = ModuleDataFlowGraph.TempVariable.newBuilder()
                        node.values.forEach {
                            tempVariableBuilder.addValues(buildEdge(it))
                        }
                        nodeBuilder.setTempVariable(tempVariableBuilder)
                    }
                }
                bodyBuilder.addNodes(nodeBuilder)
            }
            bodyBuilder.returns = nodeMap[body.returns]!!
            functionTemplateBuilder.setBody(bodyBuilder)
            moduleDFGBuilder.addFunctions(functionTemplateBuilder)
        }
    }

    fun deserialize(context: Context, startPrivateTypeIndex: Int, startPrivateFunIndex: Int): ExternalModulesDFG {
        var privateTypeIndex = startPrivateTypeIndex
        var privateFunIndex = startPrivateFunIndex
        val publicTypesMap = mutableMapOf<String, DataFlowIR.Type.Public>()
        val allTypes = mutableListOf<DataFlowIR.Type.Declared>()
        val publicFunctionsMap = mutableMapOf<String, DataFlowIR.FunctionSymbol.Public>()
        val functions = mutableMapOf<DataFlowIR.FunctionSymbol, DataFlowIR.Function>()
        val specifics = context.config.configuration.get(CommonConfigurationKeys.LANGUAGE_VERSION_SETTINGS)!!
        context.librariesWithDependencies.forEach { library ->
            val libraryDataFlowGraph = library.dataFlowGraph

            DEBUG_OUTPUT(1) {
                println("Data flow graph size for lib '${library.libraryName}': ${libraryDataFlowGraph?.size ?: 0}")
            }

            if (libraryDataFlowGraph != null) {
                val module = DataFlowIR.Module(library.moduleDescriptor(specifics))
                val moduleDataFlowGraph = ModuleDataFlowGraph.Module.parseFrom(libraryDataFlowGraph)

                val symbolTable = moduleDataFlowGraph.symbolTable
                val types = symbolTable.typesList.map {
                    when (it.typeCase) {
                        ModuleDataFlowGraph.Type.TypeCase.VIRTUAL ->
                            DataFlowIR.Type.Virtual

                        ModuleDataFlowGraph.Type.TypeCase.EXTERNAL ->
                            DataFlowIR.Type.External(it.external.name)

                        ModuleDataFlowGraph.Type.TypeCase.PUBLIC ->
                            DataFlowIR.Type.Public(it.public.name, it.public.intestines.isFinal, it.public.intestines.isAbstract).also {
                                publicTypesMap.put(it.name, it)
                                allTypes += it
                            }

                        ModuleDataFlowGraph.Type.TypeCase.PRIVATE ->
                            DataFlowIR.Type.Private(it.private.name, privateTypeIndex++, it.private.intestines.isFinal, it.private.intestines.isAbstract).also {
                                allTypes += it
                            }

                        else -> error("Unknown type: ${it.typeCase}")
                    }
                }

                val functionSymbols = symbolTable.functionSymbolsList.map {
                    when (it.functionSymbolCase) {
                        ModuleDataFlowGraph.FunctionSymbol.FunctionSymbolCase.EXTERNAL ->
                            DataFlowIR.FunctionSymbol.External(it.external.name)

                        ModuleDataFlowGraph.FunctionSymbol.FunctionSymbolCase.PUBLIC -> {
                            val symbolTableIndex = it.public.index
                            if (symbolTableIndex >= 0)
                                ++module.numberOfFunctions
                            DataFlowIR.FunctionSymbol.Public(it.public.name, module, symbolTableIndex).also {
                                publicFunctionsMap.put(it.name, it)
                            }
                        }

                        ModuleDataFlowGraph.FunctionSymbol.FunctionSymbolCase.PRIVATE -> {
                            val symbolTableIndex = it.private.index
                            if (symbolTableIndex >= 0)
                                ++module.numberOfFunctions
                            DataFlowIR.FunctionSymbol.Private(it.private.name, privateFunIndex++, module, symbolTableIndex)
                        }

                        else -> error("Unknown function symbol: ${it.functionSymbolCase}")
                    }
                }
                println("Lib: ${library.libraryName}, types: ${types.size}, functions: ${functionSymbols.size}")
                symbolTable.typesList.forEachIndexed { index, type ->
                    val deserializedType = types[index] as? DataFlowIR.Type.Declared
                            ?: return@forEachIndexed
                    val intestines = if (deserializedType is DataFlowIR.Type.Public)
                                         type.public.intestines
                                     else
                                         type.private.intestines
                    deserializedType.superTypes += intestines.superTypesList.map { types[it] }
                    deserializedType.vtable += intestines.vtableList.map { functionSymbols[it] }
                    intestines.itableList.forEach {
                        deserializedType.itable.put(it.hash, functionSymbols[it.impl])
                    }
                }

                fun deserializeEdge(edge: ModuleDataFlowGraph.Edge): DataFlowIR.Edge {
                    return DataFlowIR.Edge(if (edge.hasCastToType()) types[edge.castToType] else null)
                }

                fun deserializeCall(call: ModuleDataFlowGraph.Call): DataFlowIR.Node.Call {
                    return DataFlowIR.Node.Call(
                            functionSymbols[call.callee],
                            call.argumentsList.map { deserializeEdge(it) },
                            types[call.returnType]
                    )
                }

                fun deserializeVirtualCall(virtualCall: ModuleDataFlowGraph.VirtualCall): DataFlowIR.Node.VirtualCall {
                    val call = deserializeCall(virtualCall.call)
                    return DataFlowIR.Node.VirtualCall(
                            call.callee,
                            call.arguments,
                            call.returnType,
                            types[virtualCall.receiverType],
                            null
                    )
                }

                fun deserializeField(field: ModuleDataFlowGraph.Field): DataFlowIR.Field {
                    val type = if (field.hasType()) types[field.type] else null
                    return DataFlowIR.Field(type, field.name)
                }

                fun deserializeBody(body: ModuleDataFlowGraph.FunctionBody): DataFlowIR.FunctionBody {
                    val nodes = body.nodesList.map {
                        when (it.nodeCase) {
                            ModuleDataFlowGraph.Node.NodeCase.PARAMETER ->
                                DataFlowIR.Node.Parameter(it.parameter.index)

                            ModuleDataFlowGraph.Node.NodeCase.CONST ->
                                DataFlowIR.Node.Const(types[it.const.type])

                            ModuleDataFlowGraph.Node.NodeCase.STATICCALL -> {
                                val call = deserializeCall(it.staticCall.call)
                                val receiverType = if (it.staticCall.hasReceiverType()) types[it.staticCall.receiverType] else null
                                DataFlowIR.Node.StaticCall(call.callee, call.arguments, call.returnType, receiverType)
                            }

                            ModuleDataFlowGraph.Node.NodeCase.NEWOBJECT -> {
                                val call = deserializeCall(it.newObject.call)
                                DataFlowIR.Node.NewObject(call.callee, call.arguments, call.returnType)
                            }

                            ModuleDataFlowGraph.Node.NodeCase.VTABLECALL -> {
                                val virtualCall = deserializeVirtualCall(it.vtableCall.virtualCall)
                                DataFlowIR.Node.VtableCall(
                                        virtualCall.callee,
                                        virtualCall.receiverType,
                                        it.vtableCall.calleeVtableIndex,
                                        virtualCall.arguments,
                                        virtualCall.returnType,
                                        virtualCall.callSite
                                )
                            }

                            ModuleDataFlowGraph.Node.NodeCase.ITABLECALL -> {
                                val virtualCall = deserializeVirtualCall(it.itableCall.virtualCall)
                                DataFlowIR.Node.ItableCall(
                                        virtualCall.callee,
                                        virtualCall.receiverType,
                                        it.itableCall.calleeHash,
                                        virtualCall.arguments,
                                        virtualCall.returnType,
                                        virtualCall.callSite
                                )
                            }

                            ModuleDataFlowGraph.Node.NodeCase.SINGLETON -> {
                                DataFlowIR.Node.Singleton(types[it.singleton.type],
                                        if (it.singleton.hasConstructor()) functionSymbols[it.singleton.constructor] else null)
                            }

                            ModuleDataFlowGraph.Node.NodeCase.FIELDREAD -> {
                                val fieldRead = it.fieldRead
                                val receiver = if (fieldRead.hasReceiver()) deserializeEdge(fieldRead.receiver) else null
                                DataFlowIR.Node.FieldRead(receiver, deserializeField(fieldRead.field))
                            }

                            ModuleDataFlowGraph.Node.NodeCase.FIELDWRITE -> {
                                val fieldWrite = it.fieldWrite
                                val receiver = if (fieldWrite.hasReceiver()) deserializeEdge(fieldWrite.receiver) else null
                                DataFlowIR.Node.FieldWrite(receiver, deserializeField(fieldWrite.field), deserializeEdge(fieldWrite.value))
                            }

                            ModuleDataFlowGraph.Node.NodeCase.VARIABLE -> {
                                DataFlowIR.Node.Variable(it.variable.valuesList.map { deserializeEdge(it) })
                            }

                            ModuleDataFlowGraph.Node.NodeCase.TEMPVARIABLE -> {
                                DataFlowIR.Node.TempVariable(it.tempVariable.valuesList.map { deserializeEdge(it) })
                            }

                            else -> error("Unknown node: ${it.nodeCase}")
                        }
                    }

                    body.nodesList.forEachIndexed { index, node ->
                        val deserializedNode = nodes[index]
                        when (node.nodeCase) {
                            ModuleDataFlowGraph.Node.NodeCase.STATICCALL -> {
                                node.staticCall.call.argumentsList.forEachIndexed { i, arg ->
                                    (deserializedNode as DataFlowIR.Node.StaticCall).arguments[i].node = nodes[arg.node]
                                }
                            }

                            ModuleDataFlowGraph.Node.NodeCase.NEWOBJECT -> {
                                node.newObject.call.argumentsList.forEachIndexed { i, arg ->
                                    (deserializedNode as DataFlowIR.Node.NewObject).arguments[i].node = nodes[arg.node]
                                }
                            }

                            ModuleDataFlowGraph.Node.NodeCase.VTABLECALL -> {
                                node.vtableCall.virtualCall.call.argumentsList.forEachIndexed { i, arg ->
                                    (deserializedNode as DataFlowIR.Node.VtableCall).arguments[i].node = nodes[arg.node]
                                }
                            }

                            ModuleDataFlowGraph.Node.NodeCase.ITABLECALL -> {
                                node.itableCall.virtualCall.call.argumentsList.forEachIndexed { i, arg ->
                                    (deserializedNode as DataFlowIR.Node.ItableCall).arguments[i].node = nodes[arg.node]
                                }
                            }

                            ModuleDataFlowGraph.Node.NodeCase.FIELDREAD -> {
                                val fieldRead = node.fieldRead
                                if (fieldRead.hasReceiver())
                                    (deserializedNode as DataFlowIR.Node.FieldRead).receiver!!.node = nodes[fieldRead.receiver.node]
                            }

                            ModuleDataFlowGraph.Node.NodeCase.FIELDWRITE -> {
                                val deserializedFieldWrite = deserializedNode as DataFlowIR.Node.FieldWrite
                                val fieldWrite = node.fieldWrite
                                if (fieldWrite.hasReceiver())
                                    deserializedFieldWrite.receiver!!.node = nodes[fieldWrite.receiver.node]
                                deserializedFieldWrite.value.node = nodes[fieldWrite.value.node]
                            }

                            ModuleDataFlowGraph.Node.NodeCase.VARIABLE -> {
                                node.variable.valuesList.forEachIndexed { i, value ->
                                    (deserializedNode as DataFlowIR.Node.Variable).values[i].node = nodes[value.node]
                                }
                            }

                            ModuleDataFlowGraph.Node.NodeCase.TEMPVARIABLE -> {
                                node.tempVariable.valuesList.forEachIndexed { i, value ->
                                    (deserializedNode as DataFlowIR.Node.TempVariable).values[i].node = nodes[value.node]
                                }
                            }

                            else -> {}
                        }
                    }
                    return DataFlowIR.FunctionBody(nodes, nodes[body.returns] as DataFlowIR.Node.TempVariable)
                }

                moduleDataFlowGraph.functionsList.forEach {
                    val symbol = functionSymbols[it.symbol]
                    functions.put(symbol, DataFlowIR.Function(symbol, it.isGlobalInitializer, it.numberOfParameters, deserializeBody(it.body)))
                }
            }
        }

        return ExternalModulesDFG(allTypes, publicTypesMap, publicFunctionsMap, functions)
    }
}