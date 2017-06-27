/*
 * Copyright 2010-2017 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jetbrains.kotlin.backend.konan

import java.io.File
import org.jetbrains.kotlin.konan.target.*

class Distribution(val targetManager: TargetManager,
    val propertyFileOverride: String? = null,
    val runtimeFileOverride: String? = null) {

    val targetName = targetManager.targetName
    val hostSuffix = targetManager.hostSuffix
    val hostTargetSuffix = targetManager.hostTargetSuffix
    val targetSuffix = targetManager.targetSuffix

    private fun findUserHome() = File(System.getProperty("user.home")).absolutePath
    val userHome = findUserHome()
    val localKonanDir = "$userHome/.konan"

    private fun findKonanHome(): String {
        val value = System.getProperty("konan.home", "dist")
        val path = File(value).absolutePath 
        return path
    }

    val konanHome = findKonanHome()
    val propertyFile = propertyFileOverride ?: "$konanHome/konan/konan.properties"
    val properties = KonanProperties(propertyFile)

    val klib = "$konanHome/klib"

    val dependenciesDir = "$konanHome/dependencies"
    val dependencies = properties.propertyList("dependencies.$hostTargetSuffix")

    val stdlib = "$klib/stdlib"
    val runtime = runtimeFileOverride ?: "$stdlib/targets/${targetName}/native/runtime.bc"

    val llvmHome = "$dependenciesDir/${properties.propertyString("llvmHome.$hostSuffix")}"
    val hostSysRoot = "$dependenciesDir/${properties.propertyString("targetSysRoot.$hostSuffix")}"
    val targetSysRoot = "$dependenciesDir/${properties.propertyString("targetSysRoot.$targetSuffix")}"
    val targetToolchain = "$dependenciesDir/${properties.propertyString("targetToolchain.$hostTargetSuffix")}"
    val libffi =
            "$dependenciesDir/${properties.propertyString("libffiDir.$targetSuffix")}/lib/libffi.a"

    val llvmBin = "$llvmHome/bin"
    val llvmLib = "$llvmHome/lib"

    val llvmLto = "$llvmBin/llvm-lto"

    private val libLTODir = when (TargetManager.host) {
        KonanTarget.OSX_X8664, KonanTarget.LINUX_X8664 -> llvmLib
        KonanTarget.MINGW_X8664 -> llvmBin
        else -> error("Don't know libLTO location for this platform.")
    }
    val libLTO = "$libLTODir/${System.mapLibraryName("LTO")}"
}
