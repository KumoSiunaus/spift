package org.spift

import jcuda.runtime.JCuda._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

class cleanup extends RichFlatMapFunction
  [Boolean, Unit] {
  private var updateCount = 0

  override def flatMap(in: Boolean, out: Collector[Unit]): Unit = {
    if (in) updateCount += 1
  }

  override def close(): Unit = {
    println("Update Count: " + updateCount)
    cudaFree(resultMatrix)
  }
}