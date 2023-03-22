package org

import scala.annotation.tailrec
import scala.language.postfixOps

package object spift {
  val N = 8192

  @tailrec
  def gcd(a: Int, b: Int): Int = {
    if (b == 0) a else gcd(b, a % b)
  }

  def isColumnShift(s: Triple): Boolean = {
    (s.v == 0) || (s.u % 2 == 1 && s.v % 2 == 0) || (s.v % 2 == 0 && gcd(s.u, N) < gcd(s.v, N))
  }

  def shiftIndex(s: Triple, isCS: Boolean): Int = {
    if (s.u == 0 || s.v == 0) 0
    else if (isCS) (0 until N).find(j => s.v == j * s.u % N).get
    else (0 until N).find(k => s.u == k * s.v % N).get
  }
}