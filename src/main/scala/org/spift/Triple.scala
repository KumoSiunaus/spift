package org.spift

case class Triple(u: Int, v: Int, w: List[Double]) {
  // String representation
  override def toString: String = s"($u, $v, $w)"
}
