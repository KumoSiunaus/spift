package org.spift

import org.apache.flink.util.MathUtils


object Balancing {
  def getRebalancedKeyList(parallelism: Int = d, maxParallelism: Int = 128): Array[Int] = {
    var rebalancedKeyPartitionMap: Map[Int, Int] = Map()
    var i = 0
    while (rebalancedKeyPartitionMap.size < parallelism && i < 128) {
      val partition = keyToPartition(i, parallelism, maxParallelism)
      if (!rebalancedKeyPartitionMap.contains(partition)) {
        rebalancedKeyPartitionMap += ((partition, i))
      }
      i += 1
    }
    rebalancedKeyPartitionMap.values.toArray
  }

  def keyToPartition(key: Int, parallelism: Int = d, maxParallelism: Int = 128): Int = {
    MathUtils.murmurHash(key) % maxParallelism * parallelism / maxParallelism
  }

  def partitionToKey(partition: Int, rebalancedKeyList: Array[Int]): Int = {
    rebalancedKeyList.indexOf(partition)
  }
}