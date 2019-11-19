package com.instaclustr

import java.util.UUID
import java.util.UUID.randomUUID

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{ResultSet, Session}
import com.datastax.driver.mapping.MappingManager
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.mapper.JavaBeanColumnMapper
import com.instaclustr.model.TestModel
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.Int.{box => intBox}

/**
  * Prerequisities:
  *
  * you have to have a cluster of 3 nodes (because RF is 3)
  *
  * In this application, we just
  * 1) initialise SparkContext
  * 2) initialise CassandraConnector
  * 3) create 'keyspace' test and table 'test'
  * 4) Insert some values into test.test
  * 5) Read it back and compute a sum via Spark
  *
  * Cassandra Spark Connector uses Cassandra Java Driver of version 3.7.2
  *
  * https://github.com/datastax/spark-cassandra-connector/blob/master/project/Versions.scala#L28
  *
  * By default, if you do "CassandraConnector(sparkContext)", that connector will use
  * "DefaultConnectionFactory" (source code here (1)), that connection factory
  * uses "LocalNodeFirstLoadBalancingPolicy" (2) which is custom load balancing policy they wrote.
  *
  * using LocalNodeFirstLoadBalancingPolicy, from the documentation,
  * you see that such policy will do the following:
  *
  * Selects local node first and then nodes in local DC in random order.
  * Never selects nodes from other DCs. For writes, if a statement has a routing key set,
  * this LBP is token aware - it prefers the nodes which are replicas of the computed
  * token to the other nodes.
  *
  * This is basically what you want every time and there is not a lot of reasons to change it.
  * CHANGE IT ONLY IF YOU REALLY MUST!
  *
  * If you want to change that, you have to set property "spark.cassandra.connection.factory" in
  * your spark.properties in src/main/resources and implement your own ConnectionFactory.
  *
  * (1) https://github.com/datastax/spark-cassandra-connector/blob/master/spark-cassandra-connector/src/main/scala/com/datastax/spark/connector/cql/CassandraConnectionFactory.scala#L35-L69
  * (2) https://github.com/datastax/spark-cassandra-connector/blob/master/spark-cassandra-connector/src/main/scala/com/datastax/spark/connector/cql/CassandraConnectionFactory.scala#L50-L51
  */
object TecnotreeApp {

  private val logger = LoggerFactory.getLogger(classOf[App])

  private val keyspace = "tests"

  private val table = "test"

  private val datacenter = "dc1"

  private val replicationFactor = 3

  def main(args: Array[String]): Unit = {

    // Initialisation of SparkContext
    val sparkContext: SparkContext = new SparkContext(new SparkConf())

    // Initialisation of CassandraConnector
    val connector: CassandraConnector = CassandraConnector(sparkContext)

    try {

      //
      // via connector, create keyspace and table, if they do not exist
      //

      connector.withSessionDo(implicit session => execute(
        s"""
           |CREATE KEYSPACE IF NOT EXISTS $keyspace WITH
           |replication = {'class': 'NetworkTopologyStrategy', '$datacenter': $replicationFactor}
           |""".stripMargin))

      connector.withSessionDo(implicit session => execute(
        s"""
           |CREATE TABLE IF NOT EXISTS $keyspace.$table (id uuid, value int, primary key (id))
           |""".stripMargin))

      //
      // via connector, insert some data into test.test table via insert query, int fields "id" and "value"
      //
      connector.withSessionDo(implicit session => insert("id", "value")(randomUUID(), intBox(1)))
      connector.withSessionDo(implicit session => insert("id", "value")(randomUUID(), intBox(1)))
      connector.withSessionDo(implicit session => insert("id", "value")(randomUUID(), intBox(1)))

      //
      // inserting via mapper
      //

      val testModel = new TestModel()
      testModel.id = UUID.randomUUID()
      testModel.value = 1

      connector.withSessionDo(implicit session => insertModel(testModel))

      //
      // Just read these data back and sum "value" columns
      //
      val sum = sparkContext.cassandraTable[CassandraRow](keyspace, table).select("value").map(_.getInt("value")).sum()

      //
      // example how to read data from Cassandra table into our TestModel class
      //

      // this is implicit, it is used upon sum2 evaluation below in "cassandraTable" method
      //
      implicit val rowReaderFactory: TestModelRowReaderFactory = new TestModelRowReaderFactory

      // cassandraTable method uses rowReaderFactory for our TestModel in order to know how to convert that row from Spark to our model class
      val sum2 = sparkContext.cassandraTable[TestModel](keyspace, table).map(testModel => testModel.value).sum()

      logger.info(s"Result 1 is $sum")
      logger.info(s"Result 2 is $sum2")

      //
      // saving Java POJO bean to Cassandra, implicit columnMapper is used in saveToCassandra method
      // you have to use JavaBeanColumnMapper, if you do not use it, it will use "Scala mapper"
      // and that one will not find your getters and setters
      //

      implicit val columnMapper = new JavaBeanColumnMapper[TestModel]()

      sparkContext.parallelize[TestModel](List({
        val testModel = new TestModel

        testModel.id = UUID.randomUUID()
        testModel.value = 1

        testModel
      })).saveToCassandra(keyspace, table)

      val sum3 = sparkContext.cassandraTable[TestModel](keyspace, table).map(testModel => testModel.value).sum()

      logger.info(s"Result 3 will be '5': $sum3")
    } finally {

      //
      // After all is done, just drop that keyspace completely
      //
      connector.withSessionDo(implicit session => execute(s"DROP KEYSPACE IF EXISTS $keyspace"))

      // do not forget to stop sparkContext!
      sparkContext.stop()
    }
  }

  // helper methods

  def execute(query: String)(implicit session: Session): ResultSet = session.execute(query)

  def insertModel(testModel: TestModel)(implicit session: Session): Unit = {

    //
    // you probably want to cache this so you do not create it every time
    //

    val mappingManager = new MappingManager(session)

    val mapper = mappingManager.mapper[TestModel](classOf[TestModel])

    mapper.save(testModel)
  }

  def insert(columns: String*)(values: AnyRef*)(implicit session: Session): Option[ResultSet] = {
    if ((columns.size != values.size) || columns.isEmpty) {
      Option.empty
    } else {
      Option(session.execute(QueryBuilder.insertInto(keyspace, table).values(columns.toArray[String], values.toArray[AnyRef])))
    }
  }
}
