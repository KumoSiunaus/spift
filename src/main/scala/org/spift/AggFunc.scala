package org.spift

import org.apache.flink.api.common.functions.AggregateFunction


class AggFunc extends AggregateFunction[
  (Int, Boolean, Int, ComplexAcc),
  ComplexAcc,
  ComplexAcc
] {
  override def createAccumulator(): ComplexAcc = {
    new ComplexAcc(N * N / d)
  }

  override def merge(acc: ComplexAcc, acc1: ComplexAcc): ComplexAcc = {
    val res = new ComplexAcc(acc.length)
    (0 until N).foreach(i => {
      res.re(i) = acc.re(i) + acc1.re(i)
      res.im(i) = acc.im(i) + acc1.im(i)
    })
    res
  }

  override def add(in: (Int, Boolean, Int, ComplexAcc), acc: ComplexAcc): ComplexAcc = {
    val (key, isCS, p, q) = in
    incUpdate(acc, q, isCS, p, key)
    acc
  }

  override def getResult(acc: ComplexAcc): ComplexAcc = {
    acc
  }
}