package org

import org.spift.Balancing.getRebalancedKeyList

import scala.annotation.tailrec
import scala.language.postfixOps
import scala.math._

package object spift {
  val N = 8192
  val d = 32
  val rebalancedKeyList: Array[Int] = getRebalancedKeyList()

  @tailrec
  def gcd(a: Int, b: Int): Int = b match {
    case 0 => a
    case _ => gcd(b, a % b) // tail recursion
  }

  def W(m: Int): List[Double] = {
    List(cos(2 * Pi * m / N), sin(2 * Pi * m / N))
  }

  def keysGen[T](n: Int, e: T): Array[(Int, T)] = {
    (0 until n toArray).map(x => (rebalancedKeyList(x), e))
  }

  def isColumnShift(s: Triple): Boolean = {
    (s.u == 0) || (s.u % 2 == 1 && s.v % 2 == 0) || (s.v % 2 == 0 && gcd(s.u, N) < gcd(s.v, N))
  }

  def shiftIndex(s: Triple, isCS: Boolean): Int = {
    if (s.u == 0 || s.v == 0) 0
    else if (isCS) (0 until N).find(j => s.v == j * s.u % N).get
    else (0 until N).find(k => s.u == k * s.v % N).get
  }

  def incUpdate(I: ComplexAcc, q: ComplexAcc,
                isCS: Boolean, p: Int, key: Int): Unit = {
//    val start = System.currentTimeMillis()
    val rows = N / d
    if (isCS) {
      (0 until N).foreach(k => {
        val startIdx = ((key * rows) + (p * k)) % N
        (0 until rows).foreach(j => {
          val idx = (startIdx + j) % N
          I.re(j*N+k) += q.re(idx)
          I.im(j*N+k) += q.im(idx)
        })
      })
    }
    else {
      (0 until rows).foreach(j => {
        val startIdx = (p*(j + (key * rows))) % N
        (0 until N).foreach(k => {
          val idx = (startIdx + k) % N
          I.re(j*N+k) += q.re(idx)
          I.im(j*N+k) += q.im(idx)
        })
      })
    }

  }

  def computeVector(s: Triple, isCS: Boolean): ComplexAcc = {
    val res = new ComplexAcc(N)
    (0 until N).foreach(k => {
      if (isCS) {
        val a = s.w
        val b = W(k * s.u % N)
        res.re(k) = a.head * b.head - a.last * b.last
        res.im(k) = a.head * b.last + a.last * b.head
      }
      else {
        val a = s.w
        val b = W(k * s.v % N)
        res.re(k) = a.head * b.head - a.last * b.last
        res.im(k) = a.head * b.last + a.last * b.head
      }
    })
    res
  }
}