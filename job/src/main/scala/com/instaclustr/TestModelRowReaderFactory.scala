package com.instaclustr

import com.datastax.driver.core.Row
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.rdd.reader.{RowReader, RowReaderFactory}
import com.datastax.spark.connector.{CassandraRowMetadata, ColumnName, ColumnRef}
import com.instaclustr.model.TestModel
import com.instaclustr.model.TestModel.{ID_COLUMN, VALUE_COLUMN}

/**
  * This has to be implemented for every model class you have and use it upon cassandraTable scan as implicit
  */
class TestModelRowReaderFactory extends RowReaderFactory[TestModel] {

  override def rowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowReader[TestModel] = {
    new RowReader[TestModel] {

      // here we get a raw Row and we construct our TestModel
      override def read(row: Row, rowMetaData: CassandraRowMetadata): TestModel = {

        val testModel = new TestModel()

        testModel.id = row.getUUID(ID_COLUMN)
        testModel.value = row.getInt(VALUE_COLUMN)

        testModel
      }

      // what columns do we have?
      override def neededColumns: Option[Seq[ColumnRef]] = {
        Option(List(ColumnName(ID_COLUMN), ColumnName(VALUE_COLUMN)))
      }
    }
  }

  override def targetClass: Class[TestModel] = classOf[TestModel]
}
