package com.github.nilsga.akka.persistence.elasticsearch

import java.nio.file.Paths
import java.util.UUID

import akka.persistence.PluginSpec
import com.sksamuel.elastic4s.ElasticClient
import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.common.settings.Settings
import org.scalatest.BeforeAndAfterAll

trait ElasticsearchSetup extends BeforeAndAfterAll { this: PluginSpec =>

  var esClient : ElasticClient = _

  override protected def beforeAll() {
    val dataDir = Paths.get(System.getProperty("java.io.tmpdir")).resolve(UUID.randomUUID().toString)
    dataDir.toFile.deleteOnExit()
    dataDir.toFile.mkdirs()
    esClient = ElasticClient.local(Settings.settingsBuilder()
      .put("path.home", dataDir.toFile.getAbsolutePath)
      .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
      .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
      .build())
    super.beforeAll()
  }

  override protected def afterAll() {
    esClient.close()
    super.afterAll()
  }
}
