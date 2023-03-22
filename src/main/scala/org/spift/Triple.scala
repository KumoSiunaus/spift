package org.spift

import org.apache.commons.math3.complex.Complex

case class Triple(u: Int, v: Int, w: Complex) {
  // String representation
  override def toString: String = s"($u, $v, $w)"
}
