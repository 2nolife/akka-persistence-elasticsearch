package com.github.nilsga.akka.persistence.elasticsearch

import java.nio.file.Paths
import java.util.UUID

import akka.actor.ActorSystem
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import com.typesafe.config.Config
import org.elasticsearch.cluster.ClusterName
import org.elasticsearch.common.settings.Settings

import scala.collection.JavaConversions._

object ElasticsearchPersistencePluginConfig {

  def apply(system: ActorSystem) = new ElasticsearchPersistencePluginConfig(system.settings.config.getConfig("elasticsearch-persistence"))

}

class ElasticsearchPersistencePluginConfig(config: Config) {
  val journalType = "journal"
  val snapshotType = "snapshot"
  val index = config.getString("index")
  val cluster = config.getString("cluster")

  def createClient = config.hasPath("local") && config.getBoolean("local") match {
    case true =>
      val dataDir = Paths.get(System.getProperty("java.io.tmpdir")).resolve(UUID.randomUUID().toString)
      dataDir.toFile.deleteOnExit()
      dataDir.toFile.mkdirs()
      ElasticClient.local(Settings.settingsBuilder()
        .put("node.data", false)
        .put("node.master", false)
        .put("path.home", dataDir.toFile.getAbsolutePath)
        .build())

    case false =>
      val esSettings = Settings.settingsBuilder().put(ClusterName.SETTING, cluster).build()
      val nodes = config.getStringList("nodes").map(node => if (node.indexOf(":") >= 0) node else s"$node:9300")
      val connectionString = s"elasticsearch://${nodes.mkString(",")}"
      ElasticClient.transport(esSettings, ElasticsearchClientUri(connectionString))
  }
}
