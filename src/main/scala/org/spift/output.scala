package org.spift

import jcuda.driver.JCudaDriver.cuLaunchKernel
import jcuda.runtime.JCuda._
import jcuda.runtime.cudaMemcpyKind
import jcuda.{Pointer, Sizeof}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.mutable

class output extends RichFlatMapFunction
  [(Boolean, (Boolean, Int), Array[Double]), List[(Double, Double)]] {

  private var sum = 0
  private var updateCount = 0
  private val cleanMap: mutable.Map[(Boolean, Int), Array[Double]] = mutable.Map()

  override def open(parameters: Configuration): Unit = {
    gpuUtils()
    cudaSetDevice(1)
    System.setProperty("java.io.tmpdir", "/tmp")
  }

  override def flatMap(in: (Boolean, (Boolean, Int), Array[Double]), out: Collector[List[(Double, Double)]]): Unit = {

    sum += 1
    if (in._1) updateCount += 1
    cleanMap += (in._2 -> in._3)

    if (sum >= N * N) {
      cleanMap.foreach { case ((isCS, shiftIdx), aggShiftVec) => if (!aggShiftVec.forall(_ == 0)) {
        GPU_updateResultMat(aggShiftVec, isCS, shiftIdx)
      }
      }
      out.collect(makeResultMatrix)
    }
  }

  override def close(): Unit = {
    println("Update Count: " + updateCount)
    cudaFree(resultMatrix)
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
    else {
      val blockSizeX = 512
      val gridSizeX = Math.ceil(N.toDouble / blockSizeX).toInt
      cuLaunchKernel(
        kernel_rowUpdate,
        gridSizeX, 1, 1,
        blockSizeX, 1, 1,
        0, null,
        kernelParameters, null
      )
    }
    cudaStreamSynchronize(null)
    cudaFree(aggVecOnDevice)
  }


  private def makeResultMatrix: List[(Double, Double)] = {
    val kernelParameter = Pointer.to(
      Pointer.to(resultMatrix),
      Pointer.to(Array[Int](N * N))
    )

    val blockSizeX = 512
    val gridSizeX = Math.ceil((N * N).toDouble / blockSizeX).toInt
    cuLaunchKernel(
      kernel_makeImage,
      gridSizeX, 1, 1,
      blockSizeX, 1, 1,
      0, null,
      kernelParameter, null
    )
    cudaDeviceSynchronize

    val resArr = Array.ofDim[Double](2 * N * N)
    cudaMemcpy(
      Pointer.to(resArr), resultMatrix,
      resArr.length * Sizeof.DOUBLE,
      cudaMemcpyKind.cudaMemcpyDeviceToHost
    )
    resArr.grouped(2).map { case Array(a, b) => (a, b) }.toList
  }
}
