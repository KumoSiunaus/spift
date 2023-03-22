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

//import org.apache.flink.api.common.serialization.SimpleStringEncoder
//import org.apache.flink.core.fs.Path
//import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
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
    val path = "/opt/astrodata" + N.toString + "-100.txt"
    val src: DataStream[String] = env.readTextFile(path)

//    val sink = StreamingFileSink.forRowFormat(
//      new Path("/opt/res"),
//      new SimpleStringEncoder[(Int, List[List[Complex]])]("UTF-8"))
//      .withRollingPolicy(
//        DefaultRollingPolicy.builder()
//          .withInactivityInterval(Duration.ofSeconds(5))
//          .build())
//      .withOutputFileConfig(
//        OutputFileConfig.builder()
//          .withPartPrefix("spift")
//          .withPartSuffix(".txt")
//          .build()
//      )
//      .build()

    val res = src
      .map(s => {
      val arr = s.split(",")
      Triple(arr(0).toInt, arr(1).toInt, List(arr(2).toDouble, arr(3).toDouble))
    })
      .map(s => {
      val isCS = isColumnShift(s)
      val p = shiftIndex(s, isCS)
      (s, isCS, p)
    })
      .flatMap(e => keysGen(d, e))
      .keyBy(_._1)
      .map(new ComputeVector)
      .setParallelism(d)
      .keyBy(_._1)
      .flatMap(new ImageUpdate)
      .setParallelism(d)

//    res.addSink(sink)
//      .setParallelism(d)
    // Execute program, beginning computation.
    env.execute("Flink Scala API Skeleton")
  }
}
