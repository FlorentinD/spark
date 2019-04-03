package org.apache.spark.graph.api

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * A Property Graph as defined by the openCypher Property Graph Model.
  *
  * A graph is always tied to and managed by a [[CypherSession]]. The lifetime of a graph is bounded by the session lifetime.
  *
  * @see [[https://github.com/opencypher/openCypher/blob/master/docs/property-graph-model.adoc openCypher Property Graph Model]]
  */
trait PropertyGraph {

  /**
    * The schema (graph type) describes the structure of this graph.
    */
  def schema: PropertyGraphType

  /**
    * The session in which this graph is managed.
    */
  def cypherSession: CypherSession

  /**
    * Executes a Cypher query in the session that manages this graph, using this graph as the input graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    */
  def cypher(query: String, parameters: Map[String, Any] = Map.empty): CypherResult = cypherSession.cypher(this, query, parameters)

  /**
    * Returns the [[DataFrame]] for a given node label set.
    *
    * @param labelSet Label set used for [[DataFrame]] lookup
    * @return [[DataFrame]] for the given label set
    */
  def nodeFrame(labelSet: Set[String]): DataFrame

  /**
    * Returns the [[DataFrame]] for a given relationship type.
    *
    * @param relationshipType Relationship type used for [[DataFrame]] lookup
    * @return [[DataFrame]] for the given relationship type
    */
  def relationshipFrame(relationshipType: String): DataFrame

  /**
    * Returns a [[DataFrame]] that contains a row for each node in this graph.
    *
    * The DataFrame adheres to column naming conventions:
    *
    * {{{
    * Id column:        `$ID`
    * Label columns:    `:{LABEL_NAME}`
    * Property columns: `{Property_Key}`
    * }}}
    */
  def nodes: DataFrame

  /**
    * Returns a [[DataFrame]] that contains a row for each relationship in this graph.
    *
    * The DataFrame adheres to column naming conventions:
    *
    * {{{
    * Id column:        `$ID`
    * SourceId column:  `$SOURCE_ID`
    * TargetId column:  `$TARGET_ID`
    * RelType columns:  `:{REL_TYPE}`
    * Property columns: `{Property_Key}`
    * }}}
    */
  def relationships: DataFrame

  /**
    * Saves this graph to the given location.
    */
  def save(path: String, saveMode: SaveMode = SaveMode.ErrorIfExists): Unit =
    cypherSession.save(this, path, saveMode)

}
