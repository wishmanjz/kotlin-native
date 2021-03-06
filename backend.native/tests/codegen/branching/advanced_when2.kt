package codegen.branching.advanced_when2

import kotlin.test.*

fun advanced_when2(i: Int): Int {
  var value = 1
  when (i) {
    10 -> {val v = 42; value = v}
    11 -> {val v = 43; value = v}
    12 -> {val v = 44; value = v}
  }

  return value
}

@Test fun runTest() {
  if (advanced_when2(10) != 42) throw Error()
}
