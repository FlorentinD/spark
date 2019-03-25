package org.apache.spark.graph.api

import org.apache.spark.graph.api.GraphElementFrame.encodeIdColumns
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object GraphElementFrame {

  def encodeIdColumns(df: DataFrame, idColumnNames: String*): DataFrame = {
    val encodedIdCols = idColumnNames.map { idColumnName =>
      val col = df.col(idColumnName)
      df.schema(idColumnName).dataType match {
        case BinaryType => col
        case StringType | ByteType | ShortType | IntegerType | LongType => col.cast(BinaryType)
        // TODO: Constrain to types that make sense as IDs
        case _ => col.cast(StringType).cast(BinaryType)
      }
    }
    val remainingColumnNames = df.columns.filterNot(idColumnNames.contains)
    val remainingCols = remainingColumnNames.map(df.col)
    df.select(encodedIdCols ++ remainingCols: _*)
  }

}

/**
  * Describes how to map an input [[DataFrame]] to graph elements (i.e. nodes or relationships).
  */
trait GraphElementFrame {

  /**
    * Initial [[DataFrame]] that can still contain unmapped columns and ID columns that are not of type BinaryType.
    * The columns are ordered arbitrarily.
    */
  def initialDf: DataFrame

  /**
    * [[DataFrame]] that contains only mapped element data. Each row represents a graph element.
    * Columns in 'initialDf' that do not have BinaryType are converted to BinaryType.
    * ID columns are first, property columns are sorted alphabetically.
    */
  val df: DataFrame = {
    val mappedColumnNames = idColumns ++ properties.values.toSeq.sorted
    val mappedDf = if (mappedColumnNames == initialDf.columns.toSeq) {
      initialDf
    } else {
      initialDf.select(mappedColumnNames.map(initialDf.col): _*)
    }
    if (idColumns.forall(idColumn => initialDf.schema(idColumn).dataType == BinaryType)) {
      mappedDf
    } else {
      encodeIdColumns(mappedDf, idColumns: _*)
    }
  }

  /**
    * Name of the column that contains the graph element identifier.
    *
    * @note Column values need to be of [[org.apache.spark.sql.types.BinaryType]].
    */
  def idColumn: String

  /**
    * Mapping from graph element property keys to the columns that contain the corresponding property values.
    */
  def properties: Map[String, String]

  protected def idColumns: Seq[String]

}

object NodeFrame {

  /**
    * Describes how to map an input [[DataFrame]] to nodes.
    *
    * All columns apart from the given `idColumn` are mapped to node properties.
    *
    * @param df       [[DataFrame]] containing a single node in each row
    * @param idColumn column that contains the node identifier
    * @param labels   labels that are assigned to all nodes
    */
  def apply(
    df: DataFrame,
    idColumn: String,
    labels: Set[String] = Set.empty
  ): NodeFrame = {
    val properties = (df.columns.toSet - idColumn)
      .map(columnName => columnName -> columnName).toMap
    NodeFrame(df, idColumn, labels, properties)
  }

}

/**
  * Describes how to map an input [[DataFrame]] to nodes.
  *
  * @param initialDf  [[DataFrame]] containing a single node in each row
  * @param idColumn   column that contains the node identifier
  * @param labels     labels that are assigned to all nodes
  * @param properties mapping from property keys to corresponding columns
  */
case class NodeFrame(
  initialDf: DataFrame,
  idColumn: String,
  labels: Set[String],
  properties: Map[String, String]
) extends GraphElementFrame {

  override protected def idColumns: Seq[String] = Seq(idColumn)

}

object RelationshipFrame {

  /**
    * Describes how to map an input [[DataFrame]] to relationships.
    *
    * All columns apart from the given identifier columns are mapped to relationship properties.
    *
    * @param initialDf        [[DataFrame]] containing a single relationship in each row
    * @param idColumn         column that contains the relationship identifier
    * @param sourceIdColumn   column that contains the source node identifier of the relationship
    * @param targetIdColumn   column that contains the target node identifier of the relationship
    * @param relationshipType relationship type that is assigned to all relationships
    */
  def apply(
    initialDf: DataFrame,
    idColumn: String,
    sourceIdColumn: String,
    targetIdColumn: String,
    relationshipType: String
  ): RelationshipFrame = {
    val properties = (initialDf.columns.toSet - idColumn - sourceIdColumn - targetIdColumn)
      .map(columnName => columnName -> columnName).toMap
    RelationshipFrame(initialDf, idColumn, sourceIdColumn, targetIdColumn, relationshipType, properties)
  }

}

/**
  * Describes how to map an input [[DataFrame]] to relationships.
  *
  * @param initialDf        [[DataFrame]] containing a single relationship in each row
  * @param idColumn         column that contains the relationship identifier
  * @param sourceIdColumn   column that contains the source node identifier of the relationship
  * @param targetIdColumn   column that contains the target node identifier of the relationship
  * @param relationshipType relationship type that is assigned to all relationships
  * @param properties       mapping from property keys to corresponding columns
  */
case class RelationshipFrame(
  initialDf: DataFrame,
  idColumn: String,
  sourceIdColumn: String,
  targetIdColumn: String,
  relationshipType: String,
  properties: Map[String, String]
) extends GraphElementFrame {

  override protected def idColumns: Seq[String] = Seq(idColumn, sourceIdColumn, targetIdColumn)

}