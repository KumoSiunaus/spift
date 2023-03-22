package org.spift

import jcuda.driver.JCudaDriver._
import jcuda.driver._
import jcuda.runtime.JCuda._
import jcuda.runtime.cudaMemcpyKind
import jcuda.{Pointer, Sizeof}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

class ComputeVector extends RichMapFunction
  [(Triple, Boolean, Int),
    (Boolean, Int, Array[Double])] {

  var matrixPointer: Pointer = _

  private var module: CUmodule = _
  private var kernelFunc: CUfunction = _

  var res: Int = -1

  override def open(parameters: Configuration): Unit = {

    import java.util.UUID
    val originTempDir = System.getProperty("java.io.tmpdir")
    val newTempDir = originTempDir + "/jcuda-" + UUID.randomUUID
    System.setProperty("java.io.tmpdir", newTempDir)

    cudaSetDevice(1)
    cudaFree(new Pointer) // wake-up and creat context

    // for debug
    JCudaDriver.setExceptionsEnabled(true)

    module = new CUmodule
    res = cuModuleLoad(module, "/opt/spift2.ptx")
    kernelFunc = new CUfunction
    res = cuModuleGetFunction(kernelFunc, module, "computeVec")

    matrixPointer = new Pointer
    res = cudaMalloc(matrixPointer, N * Sizeof.DOUBLE * 2)
    res = cudaMemset(matrixPointer, 0, N * Sizeof.DOUBLE * 2)

    System.setProperty("java.io.tmpdir", "/tmp")
  }

  override def map(in: (Triple, Boolean, Int)): (Boolean, Int, Array[Double]) = {
    val (s, isCS, shiftIdx) = in

    val kernelParameters = Pointer.to(
      Pointer.to(Array[Int](N)),
      Pointer.to(matrixPointer),
      Pointer.to(Array[Int](s.u)),
      Pointer.to(Array[Int](s.v)),
      Pointer.to(Array[Double](s.w.head, s.w.last)),
      Pointer.to(Array[Int](if (isCS) 1 else 0))
    )

    val blockSizeX = 512
    val gridSizeX = Math.ceil(N.toDouble / blockSizeX).toInt
    res = cuLaunchKernel(
      kernelFunc,
      gridSizeX, 1, 1,
      blockSizeX, 1, 1,
      0, null,
      kernelParameters, null
    )
    cuCtxSynchronize

    val q = new Array[Double](N * 2)
    cudaMemcpy(
      Pointer.to(q), matrixPointer,
      N * Sizeof.DOUBLE * 2,
      cudaMemcpyKind.cudaMemcpyDeviceToHost
    )

    (isCS, shiftIdx, q)
  }

  override def close(): Unit = {
    cudaFree(matrixPointer)
  }
}