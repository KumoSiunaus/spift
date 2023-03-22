package org.spift

import jcuda.driver.JCudaDriver.cuLaunchKernel
import jcuda.runtime.JCuda._
import jcuda.runtime.{cudaMemcpyKind, cudaStreamCallback, cudaStream_t}
import jcuda.{Pointer, Sizeof}
import org.apache.commons.math3.complex.Complex
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import java.util

class ComputeMatrix extends RichFlatMapFunction
  [(Triple, (Boolean, Int)), Boolean] {

  private val warr: Array[Complex] = Array.tabulate(N)(i => W(i))
  private var aggShiftVec: ValueState[Array[Double]] = _

  override def open(parameters: Configuration): Unit = {
    gpuUtils()
    cudaSetDevice(1)


    aggShiftVec = getRuntimeContext.getState(new ValueStateDescriptor[Array[Double]](
      "aggShiftVec", classOf[Array[Double]]
    ))

    System.setProperty("java.io.tmpdir", "/tmp")
  }

  override def flatMap(in: (Triple, (Boolean, Int)),
                       collector: Collector[Boolean]): Unit = {
    if (aggShiftVec.value == null) aggShiftVec.update(new Array[Double](N * 2))

    val (s, (isCS, shiftIdx)) = in
    CPU_addToAggShiftVec(aggShiftVec.value, s, isCS)

    var isUpdated = false
    lock.lock()
    if (!isUpdating) {
      isUpdating = true
      lock.unlock()
      GPU_updateResultMat(aggShiftVec.value, isCS, shiftIdx)
      util.Arrays.fill(aggShiftVec.value, 0)
      isUpdated = true
    }
    else if (lock.isLocked) lock.unlock()

    collector.collect(isUpdated)
  }

  private def CPU_addToAggShiftVec(agg: Array[Double], s: Triple, isCS: Boolean): Unit = {
    val arr = computeVec(s, isCS, warr)
    for (i <- agg.indices) agg(i) += arr(i)
  }

  private def GPU_updateResultMat(aggVecOnHost: Array[Double], isCS: Boolean, shiftIdx: Int): Unit = {
    val aggVecOnDevice = new Pointer
    cudaMalloc(aggVecOnDevice, N * Sizeof.DOUBLE * 2)
    cudaMemcpy(aggVecOnDevice, Pointer.to(aggVecOnHost),
      N * Sizeof.DOUBLE * 2, cudaMemcpyKind.cudaMemcpyHostToDevice
    )

    val kernelParameters = Pointer.to(
      Pointer.to(Array[Int](N)),
      Pointer.to(resultMatrix),
      Pointer.to(aggVecOnDevice),
      Pointer.to(Array[Int](shiftIdx))
    )

    if (isCS) {
      val blockSizeX = 32
      val gridSizeX = Math.ceil(N.toDouble / blockSizeX).toInt
      cuLaunchKernel(
        kernel_colUpdate,
        gridSizeX, gridSizeX, 1,
        blockSizeX, blockSizeX, 1,
        blockSizeX * blockSizeX * Sizeof.DOUBLE * 2, null,
        kernelParameters, null
      )
    }
    //    else {
    //      val blockSizeX = 512
    //      val gridSizeX = Math.ceil(N.toDouble / blockSizeX).toInt
    //      cuLaunchKernel(
    //        kernel_rowUpdate,
    //        gridSizeX, 1, 1,
    //        blockSizeX, 1, 1,
    //        0, null,
    //        kernelParameters, null
    //      )
    //    }
    else {
      val blockSizeX = 32
      val gridSizeX = Math.ceil(N.toDouble / blockSizeX).toInt
      cuLaunchKernel(
        kernel_rowUpdate,
        gridSizeX, gridSizeX, 1,
        blockSizeX, blockSizeX, 1,
        0, null,
        kernelParameters, null
      )
    }
    //    cudaStreamSynchronize(null)
    cudaStreamAddCallback(null, new Callback, aggVecOnDevice, 0)
  }

  private class Callback extends cudaStreamCallback {
    override def call(cudaStream_t: cudaStream_t, i: Int, o: Any): Unit = {
      cudaFree(o.asInstanceOf[Pointer])
      lock.lock()
      isUpdating = false
      lock.unlock()
    }
  }
}
