package org.apache.spark.cypher.adapters

import org.apache.spark.cypher.adapters.MappingAdapter._
import org.apache.spark.cypher.{SparkCypherSession, SparkEntityTable}
import org.apache.spark.graph.api.{PropertyGraph, PropertyGraphType, _}
import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.ir.api.expr.Var

case class RelationalGraphAdapter(
  cypherSession: SparkCypherSession,
  nodeFrames: Seq[DataFrame],
  relationshipFrames: Seq[DataFrame]) extends PropertyGraph {

  override def schema: PropertyGraphType = SchemaAdapter(graph.schema)

  private[cypher] lazy val graph = {
    if (nodeFrames.isEmpty) {
      cypherSession.graphs.empty
    } else {
      val nodeTables = nodeFrames.map { nodeDataFrame => SparkEntityTable(nodeDataFrame.toNodeMapping, nodeDataFrame) }
      val relTables = relationshipFrames.map { relDataFrame => SparkEntityTable(relDataFrame.toRelationshipMapping, relDataFrame) }
      cypherSession.graphs.create(nodeTables.head, nodeTables.tail ++ relTables: _*)
    }
  }

  private lazy val _nodeFrame: Map[Set[String], DataFrame] = nodeFrames.map(nf => nf.labels -> nf).toMap

  private lazy val _relationshipFrame: Map[String, DataFrame] = relationshipFrames.map(rf => rf.relationshipType -> rf).toMap

  override def nodes: DataFrame = {
    // TODO: move to API as default implementation
    val nodeVar = Var("n")(CTNode)
    val nodes = graph.nodes(nodeVar.name)

    val df = nodes.table.df
    val header = nodes.header

    val idRename = header.column(nodeVar) -> "$ID"
    val labelRenames = header.labelsFor(nodeVar).map(hasLabel => header.column(hasLabel) -> s":${hasLabel.label.name}").toSeq.sortBy(_._2)
    val propertyRenames = header.propertiesFor(nodeVar).map(property => header.column(property) -> property.key.name).toSeq.sortBy(_._2)

    val selectColumns = (Seq(idRename) ++ labelRenames ++ propertyRenames).map { case (oldColumn, newColumn) => df.col(oldColumn).as(newColumn) }

    df.select(selectColumns: _*)
  }

  override def relationships: DataFrame = {
    // TODO: move to API as default implementation
    val relVar = Var("r")(CTRelationship)
    val rels = graph.relationships(relVar.name)

    val df = rels.table.df
    val header = rels.header

    val idRename = header.column(relVar) -> "$ID"
    val sourceIdRename = header.column(header.startNodeFor(relVar)) -> "$SOURCE_ID"
    val targetIdRename = header.column(header.endNodeFor(relVar)) -> "$TARGET_ID"
    val relTypeRenames = header.typesFor(relVar).map(hasType => header.column(hasType) -> s":${hasType.relType.name}").toSeq.sortBy(_._2)
    val propertyRenames = header.propertiesFor(relVar).map(property => header.column(property) -> property.key.name).toSeq.sortBy(_._2)

    val selectColumns = (Seq(idRename, sourceIdRename, targetIdRename) ++ relTypeRenames ++ propertyRenames).map { case (oldColumn, newColumn) => df.col(oldColumn).as(newColumn) }

    df.select(selectColumns: _*)
  }

  override def nodeFrame(labelSet: Set[String]): DataFrame = _nodeFrame(labelSet)

  override def relationshipFrame(relationshipType: String): DataFrame = _relationshipFrame(relationshipType)

}
