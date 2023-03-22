package org.spift

import jcuda.driver.{CUfunction, CUmodule, JCudaDriver}
import jcuda.driver.JCudaDriver.{cuCtxSynchronize, cuLaunchKernel, cuModuleGetFunction, cuModuleLoad}
import jcuda.runtime.JCuda._
import jcuda.runtime.cudaMemcpyKind
import jcuda.{Pointer, Sizeof}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class cleanup extends RichFlatMapFunction
  [Pointer, List[(Double, Double)]] {

  private var module: CUmodule = _
  private var resultMatrix: Pointer = _
  private var kernel_makeImage: CUfunction = _

  override def open(parameters: Configuration): Unit = {
    import java.util.UUID
    val originTempDir = System.getProperty("java.io.tmpdir")
    val newTempDir = originTempDir + "/jcuda-" + UUID.randomUUID
    System.setProperty("java.io.tmpdir", newTempDir)

    JCudaDriver.setExceptionsEnabled(true)
    cudaSetDevice(0)
    cudaFree(new Pointer)

    module = new CUmodule
    cuModuleLoad(module, "/opt/spift.ptx")
    kernel_makeImage = new CUfunction
    cuModuleGetFunction(kernel_makeImage, module, "makeImage")

    resultMatrix = new Pointer

    System.setProperty("java.io.tmpdir", originTempDir)
  }

  override def flatMap(in: Pointer, out: Collector[List[(Double, Double)]]): Unit = {

    resultMatrix = in

    val kernelParameter = Pointer.to(
      Pointer.to(resultMatrix),
      Pointer.to(Array[Int](N * N))
    )

    val blockSizeX = 256
    val gridSizeX = Math.ceil((2 * N * N).toDouble / blockSizeX).toInt

    cuLaunchKernel(
      kernel_makeImage,
      gridSizeX, 1, 1,
      blockSizeX, 1, 1,
      0, null,
      kernelParameter, null
    )
    cuCtxSynchronize

    val resArr = Array.ofDim[Double](2 * N * N)
    cudaMemcpy(
      Pointer.to(resArr), resultMatrix,
      resArr.length * Sizeof.DOUBLE,
      cudaMemcpyKind.cudaMemcpyDeviceToHost
    )
    out.collect(resArr.grouped(2).map { case Array(a, b) => (a, b) }.toList)
  }

  override def close(): Unit = {
    //    val arr = new Array[Double](2)
    //    cudaMemcpy(
    //      Pointer.to(arr), resultMatrix,
    //      2 * Sizeof.DOUBLE,
    //      cudaMemcpyKind.cudaMemcpyDeviceToHost
    //    )
    //    println(arr(0), arr(1))
    cudaFree(resultMatrix)
  }
}