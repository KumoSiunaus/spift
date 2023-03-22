package org.spift

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

class ComputeVector extends RichMapFunction
  [(Int, (Triple, Boolean, Int)),
    (Int, (Boolean, Int, ComplexAcc))]{

  private var pid: Int = _

  override def map(in: (Int, (Triple, Boolean, Int))): (Int, (Boolean, Int, ComplexAcc)) = {
    val key = in._1
    val (s, isCS, p) = in._2
    val q = computeVector(s, isCS)
    if (pid == 3) println(q.re(0), q.im(0))
    (key, (isCS, p, q))
  }

  override def open(parameters: Configuration): Unit = {
    pid = getRuntimeContext.getIndexOfThisSubtask
  }

}
