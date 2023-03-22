package org.spift

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.spift.Balancing.partitionToKey

class ImageUpdate extends RichFlatMapFunction
  [(Int, (Boolean, Int, ComplexAcc)),
    (Int, ComplexAcc)] {

  type IN = (Int, Boolean, Int, ComplexAcc)
  type ACC = ComplexAcc
  type OUT = ComplexAcc

  val aggFunc = new AggFunc

  private var I: AggregatingState[IN, OUT] = _
  private var pid: Int = _

  override def flatMap(in: (Int, (Boolean, Int, ComplexAcc)),
                       collector: Collector[(Int, ComplexAcc)]): Unit = {
    val (rebalancedKey, (isCS, p, q)) = in
    val key = partitionToKey(rebalancedKey, rebalancedKeyList)
    I.add(key, isCS, p, q)
    collector.collect((key, I.get()))
  }

  override def open(parameters: Configuration): Unit = {
    pid = getRuntimeContext.getIndexOfThisSubtask
    I = getRuntimeContext.getAggregatingState(
      new AggregatingStateDescriptor[IN, ACC, OUT]("getImg", aggFunc, createTypeInformation[ACC])
    )
  }
}