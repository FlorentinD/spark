package org.apache.spark.cypher

import org.apache.spark.SparkFunSuite
import org.apache.spark.graph.api._
import org.apache.spark.sql.DataFrame

class GraphExamplesSuite extends SparkFunSuite with SharedCypherContext {

//  test("match single node pattern using spark-graph-api") {
//    val nodeData: DataFrame = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob")).toDF("id", "name")
//    val nodeDataFrame: NodeFrame = NodeFrame(df = nodeData, idColumn = "id", labels = Set("Person"))
//    val graph: PropertyGraph = cypherSession.createGraph(Seq(nodeDataFrame))
//    val result: CypherResult = graph.cypher("MATCH (n) RETURN n")
//    result.df.show()
//  }

  test("write/read metadata") {
    spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob"))
      .toDF(idColumnName, "name")
      .toNodeFrame("Person")
      .write
      .parquet("foo")
    println(spark.read.parquet("foo").labels)
  }

  test("df with meta data") {
    val persons: DataFrame = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob"))
      .toDF(idColumnName, "name")
      .toNodeFrame("Person")
    val knows: DataFrame = spark.createDataFrame(Seq((0, 0, 1)))
      .toDF(idColumnName, sourceIdColumnName, targetIdColumnName)
      .toRelationshipFrame("KNOWS")

    val graph: PropertyGraph = cypherSession.createGraph(persons, knows)
    val result: CypherResult = graph.cypher(
      """
        |MATCH (a:Person)-[:KNOWS]->(b:Person)
        |RETURN a.name AS person1, b.name AS person2""".stripMargin)

    result.df.show()
    println(graph.schema.labelSets)
    println(graph.schema.relationshipTypes)
    graph.schema.labelSets.map(graph.nodeFrame).foreach(_.schema.printTreeString())
  }

//  test("match simple pattern using spark-graph-api") {
//    val nodeData: DataFrame = spark.createDataFrame(Seq(0 -> "Alice", 1 -> "Bob")).toDF("id", "name")
//    val relationshipData: DataFrame = spark.createDataFrame(Seq(Tuple3(0, 0, 1))).toDF("id", "source", "target")
//    val nodeDataFrame: NodeFrame = NodeFrame(df = nodeData, idColumn = "id", labels = Set("Person"))
//    val relationshipFrame: RelationshipFrame = RelationshipFrame(relationshipData, idColumn = "id", sourceIdColumn = "source", targetIdColumn = "target", relationshipType = "KNOWS")
//
//    val graph: PropertyGraph = cypherSession.createGraph(Seq(nodeDataFrame), Seq(relationshipFrame))
//    graph.nodes.show()
//    graph.relationships.show()
//
//    graph.cypher("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS person1, b.name AS person2").df.show()
//  }
//
//  test("round trip example using column name conventions") {
//    val graph1: PropertyGraph = cypherSession.createGraph(nodes, relationships)
//    val graph2: PropertyGraph = cypherSession.createGraph(graph1.nodes, graph1.relationships)
//    graph2.nodes.show()
//    graph2.relationships.show()
//  }
//
//  test("example for retaining user ids") {
//    val nodesWithRetainedId = nodes.withColumn("retainedId", nodes.col("$ID"))
//    val relsWithRetainedId = relationships.withColumn("retainedId", relationships.col("$ID"))
//
//    cypherSession
//      .createGraph(nodesWithRetainedId, relsWithRetainedId)
//      .cypher("MATCH (n:Student)-[:STUDY_AT]->(u:University) RETURN n, u").df.show()
//  }
//
//  lazy val nodes: DataFrame = spark.createDataFrame(Seq(
//    (0L, true, false, Some("Alice"), Some(42), None),
//    (1L, true, false, Some("Bob"), Some(23), None),
//    (2L, true, false, Some("Carol"), Some(22), None),
//    (3L, true, false, Some("Eve"), Some(19), None),
//    (4L, false, true, None, None, Some("UC Berkeley")),
//    (5L, false, true, None, None, Some("Stanford"))
//  )).toDF("$ID", ":Student", ":University", "name", "age", "title")
//
//  lazy val relationships: DataFrame = spark.createDataFrame(Seq(
//    (0L, 0L, 1L, true, false),
//    (1L, 0L, 3L, true, false),
//    (2L, 1L, 3L, true, false),
//    (3L, 3L, 0L, true, false),
//    (4L, 3L, 1L, true, false),
//    (5L, 0L, 4L, false, true),
//    (6L, 1L, 4L, false, true),
//    (7L, 3L, 4L, false, true),
//    (8L, 2L, 5L, false, true),
//  )).toDF("$ID", "$SOURCE_ID", "$TARGET_ID", ":KNOWS", ":STUDY_AT")
}
