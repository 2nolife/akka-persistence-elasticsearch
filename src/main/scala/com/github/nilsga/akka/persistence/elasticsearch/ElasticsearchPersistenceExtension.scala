package com.github.nilsga.akka.persistence.elasticsearch

import java.util.concurrent.Executors

import akka.actor._
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._

object ElasticsearchPersistenceExtension extends ExtensionId[ElasticsearchPersistenceExtensionImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem) = new ElasticsearchPersistenceExtensionImpl(system)

  override def lookup() = ElasticsearchPersistenceExtension
}

class ElasticsearchPersistenceExtensionImpl(val system: ActorSystem) extends Extension {

  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  private implicit val plugin = this

  private val createTimeout = 10 seconds
  val config = ElasticsearchPersistencePluginConfig(system)
  lazy val client = config.createClient

  import ElasticsearchPersistenceMappings._
  Await.result(ensureJournalMappingExists(), createTimeout)
  Await.result(ensureSnapshotMappingExists(), createTimeout)

}
