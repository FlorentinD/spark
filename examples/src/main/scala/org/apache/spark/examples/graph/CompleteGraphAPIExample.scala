/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.graph

// $example on$

import org.apache.spark.cypher.SparkCypherSession
import org.apache.spark.graph.api.PropertyGraph
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
// $example off$

object CompleteGraphAPIExample {

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // Initialise a GraphSession
    val cypherSession = SparkCypherSession.create(spark)
    // TODO: load via CSV (2 nodeFrames and 2 relationship frames ?)
    // Create node df and edge df
    val nodeData: DataFrame = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob"))
      .toDF("id", "name")
    val relationshipData: DataFrame = spark.createDataFrame(Seq((0, 0, 1)))
      .toDF("id", "source", "target")

    // TODO: create specific node and relationship frames

    // Create a PropertyGraph
    val graph: PropertyGraph = cypherSession.createGraph(nodeData, relationshipData)

    // Show graph API
    graph.schema

    graph.relationshipFrame("KNOWS")
    graph.nodeFrame(Array("Employee", "Employer"))
    graph.cypher(
      """
        |MATCH (a:Person)-[r:KNOWS]->(:Person)
        |WHERE a.name = $name
        |RETURN a, r""".stripMargin, Map("name" -> "Bob"))

    // Store the PropertyGraph
    val path = "examples/src/main/resources/exampleGraph"
    graph.save(path, SaveMode.Ignore)

    // Load the PropertyGraph
    val importedGraph = cypherSession.load(path)


    spark.stop()
  }
}

