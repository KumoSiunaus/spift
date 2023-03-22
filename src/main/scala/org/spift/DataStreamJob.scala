/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.spift

import org.apache.commons.math3.complex.Complex
import org.apache.flink.streaming.api.scala._


/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object DataStreamJob {
  def main(args: Array[String]): Unit = {
    // Sets up the execution environment, which is the main entry point
    // to building Flink applications.
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(16)
    val path = "/opt/astrodata" + N.toString + "-100k.txt"
    //    val path = "/opt/lena-512.txt"
    val src: DataStream[String] = env.readTextFile(path)

    //    val sink = FileSink
    //      .forRowFormat(
    //        new Path("/opt/res"),
    //        new SimpleStringEncoder[List[(Double, Double)]]("UTF-8"))
    //      .withRollingPolicy(
    //        DefaultRollingPolicy.builder()
    //          .build())
    //      .withOutputFileConfig(
    //        OutputFileConfig.builder()
    //          .withPartPrefix("spift")
    //          .withPartSuffix(".txt")
    //          .build()
    //      )
    //      .build()

    src
      .map(s => {
        val arr = s.split(",")
        Triple(arr(0).toInt, arr(1).toInt, new Complex(arr(2).toDouble, arr(3).toDouble))
      })
      .map(s => {
        val isCS = isColumnShift(s)
        val shiftIdx = shiftIndex(s, isCS)
        (s, (isCS, shiftIdx))
      })
      .keyBy(_._2)
      .flatMap(new ComputeMatrix)
      .flatMap(new cleanup)
      .setParallelism(1)
    //      .sinkTo(sink)
    //      .setParallelism(1)

    // Execute program, beginning computation.
    env.execute("Flink Scala API Skeleton")

    System.setProperty("java.io.tmpdir", "/tmp")
  }
}
