package com.instaclustr

import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.cql.{CassandraConnectionFactory, CassandraConnectorConf, DefaultConnectionFactory}

/**
  * This is custom way how to implement CassandraConnectionFactory which will
  * be used by Cassandra Spark Connector to create cluster object to talk to Cassandra.
  *
  * To use this custom factory, you have to specify property "spark.cassandra.connection.factory"
  * in spark.properties in src/main/resources of the "job" artifact.
  *
  * At the bottom of that property file, you have that property set which points to this class.
  *
  * Put the jar in target for this connection-factory project on the classpath of Spark. (e.g. put it into "jars" directory of Spark)
  */
class TecnotreeConnectionFactory extends CassandraConnectionFactory {

  override def createCluster(conf: CassandraConnectorConf): Cluster =
    DefaultConnectionFactory.clusterBuilder(conf)
      // if you want to use your own policy, just uncomment this and set it as you want
      // however, keep in mind that custom policy is doing its best to provide you the
      // best experience for Cassandra Spark Driver so it is very questionable why you are doing this!
      //
      // By default, it is uses LocalNodeFirstLoadBalancingPolicy
      //
      //.withLoadBalancingPolicy()
      .build()
}
