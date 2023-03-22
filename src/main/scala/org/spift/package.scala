package org

import jcuda.driver.JCudaDriver.{cuModuleGetFunction, cuModuleLoad}
import jcuda.driver.{CUfunction, CUmodule, JCudaDriver}
import jcuda.runtime.JCuda.{cudaFree, cudaMalloc, cudaMemset, cudaSetDevice}
import jcuda.{Pointer, Sizeof}
import org.apache.commons.math3.complex.Complex
import org.apache.commons.math3.util.FastMath.{cos, sin}

import java.util.concurrent.locks.ReentrantLock
import scala.annotation.tailrec
import scala.language.postfixOps
import scala.math.Pi

package object spift {

  gpuUtils()
  private var res: Int = -1

  // for debug
  JCudaDriver.setExceptionsEnabled(true)

  cudaSetDevice(1)
  cudaFree(new Pointer) // wake-up and creat context

  val N = 8192
  var isUpdating = false
  val lock = new ReentrantLock

  val module = new CUmodule
  res = cuModuleLoad(module, "/opt/spift2.ptx")
  val kernel_computeVec = new CUfunction
  res = cuModuleGetFunction(kernel_computeVec, module, "computeVec")
  val kernel_colUpdate = new CUfunction
  res = cuModuleGetFunction(kernel_colUpdate, module, "colUpdate")
  val kernel_rowUpdate = new CUfunction
  res = cuModuleGetFunction(kernel_rowUpdate, module, "rowUpdate2")
  val kernel_makeImage = new CUfunction
  res = cuModuleGetFunction(kernel_makeImage, module, "makeImage")

  val resultMatrix = new Pointer
  res = cudaMalloc(resultMatrix, N * N * Sizeof.DOUBLE * 2)
  res = cudaMemset(resultMatrix, 0, N * N * Sizeof.DOUBLE * 2)

  @tailrec
  def gcd(a: Int, b: Int): Int = {
    if (b == 0) a else gcd(b, a % b)
  }

  def extendedGcd(a: Int, b: Int): (Int, Int, Int) = {
    if (b == 0) (a, 1, 0) else {
      val (d, x, y) = extendedGcd(b, a % b)
      (d, y, x - a / b * y)
    }
  }

  def W(m: Int): Complex = {
    Complex.I
      .multiply(sin(2 * Pi * m / N))
      .add(cos(2 * Pi * m / N))
  }

  def isColumnShift(s: Triple): Boolean = {
    (s.v == 0) || (s.u % 2 == 1 && s.v % 2 == 0) || (s.v % 2 == 0 && gcd(s.u, N) < gcd(s.v, N))
  }

  def shiftIndex_old(s: Triple, isCS: Boolean): Int = {
    if (s.u == 0 || s.v == 0) 0
    else if (isCS) (0 until N).find(j => s.v == j * s.u % N).get
    else (0 until N).find(k => s.u == k * s.v % N).get
  }

  def shiftIndex(s: Triple, isCS: Boolean): Int = {
    if (s.u == 0 || s.v == 0) 0
    else {
      if (isCS) {
        val (d, x, _) = extendedGcd(s.u, N)
        Math.floorMod(s.v / d * x, N / d)
      }
      else {
        val (d, x, _) = extendedGcd(s.v, N)
        Math.floorMod(s.u / d * x, N / d)
      }
    }
  }

  def computeVec_old(s: Triple, isCS: Boolean): Array[Double] = {
    val res = new Array[Double](N * 2)
    for (i <- 0 until N) {
      val v = if (isCS) s.w.multiply(W((i * s.u) % N)) else s.w.multiply(W((i * s.v) % N))
      res(i * 2) = v.getReal
      res(i * 2 + 1) = v.getImaginary
    }
    res
  }

  def computeVec(s: Triple, isCS: Boolean, warr: Array[Complex]): Array[Double] = {
    val res = new Array[Double](N * 2)
    for (i <- 0 until N) {
      val v = if (isCS) s.w.multiply(warr((i * s.u) % N)) else s.w.multiply(warr((i * s.v) % N))
      res(i * 2) = v.getReal
      res(i * 2 + 1) = v.getImaginary
    }
    res
  }


  def gpuUtils(): String = {
    System.setProperty("java.io.tmpdir", "/tmp")
    import java.util.UUID
    val originTempDir = System.getProperty("java.io.tmpdir")
    val newTempDir = originTempDir + "/jcuda-" + UUID.randomUUID
    System.setProperty("java.io.tmpdir", newTempDir)
    originTempDir
  }
}