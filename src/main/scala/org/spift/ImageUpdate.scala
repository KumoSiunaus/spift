package org.spift

import jcuda.driver.JCudaDriver._
import jcuda.driver._
import jcuda.runtime.JCuda._
import jcuda.runtime.cudaMemcpyKind
import jcuda.{Pointer, Sizeof}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

class ImageUpdate extends RichMapFunction
  [(Boolean, Int, Array[Double]), Pointer] {

  private var module: CUmodule = _
  private var matrixPointer: Pointer = _
  private var kernel_incUpdate: CUfunction = _

  override def open(parameters: Configuration): Unit = {
    import java.util.UUID
    val originTempDir = System.getProperty("java.io.tmpdir")
    val newTempDir = originTempDir + "/jcuda-" + UUID.randomUUID
    System.setProperty("java.io.tmpdir", newTempDir)

    JCudaDriver.setExceptionsEnabled(true)
    cudaSetDevice(1)
    cudaFree(new Pointer)
    
    module = new CUmodule
    cuModuleLoad(module, "/opt/spift2.ptx")
    kernel_incUpdate = new CUfunction
    cuModuleGetFunction(kernel_incUpdate, module, "incUpdate")

    matrixPointer = new Pointer
    cudaMalloc(matrixPointer, 2 * N * N * Sizeof.DOUBLE)
    cudaMemset(matrixPointer, 0, 2 * N * N * Sizeof.DOUBLE)

    System.setProperty("java.io.tmpdir", "/tmp")
  }

  override def map(in: (Boolean, Int, Array[Double])): Pointer = {

    val (isCS, p, q) = in

    // Copy q to device
    val pq = new Pointer
    cudaMalloc(pq, q.length * Sizeof.DOUBLE)
    cudaMemcpy(
      pq, Pointer.to(q),
      q.length * Sizeof.DOUBLE,
      cudaMemcpyKind.cudaMemcpyHostToDevice
    )

    // Set up the kernel parameters.
    val kernelParameters = Pointer.to(
      Pointer.to(Array[Int](N)),
      Pointer.to(matrixPointer),
      Pointer.to(pq),
      Pointer.to(Array[Int](if (isCS) 1 else 0)),
      Pointer.to(Array[Int](p))
    )

    val blockSizeX = 512
    val gridSizeX = Math.ceil((N * N).toDouble / blockSizeX).toInt
    cuLaunchKernel(
      kernel_incUpdate,
      gridSizeX, 1, 1,
      blockSizeX, 1, 1,
      0, null,
      kernelParameters, null
    )
    cuCtxSynchronize

    // Cleanup
    cudaFree(pq)

    matrixPointer
  }
}