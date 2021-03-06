package com.github.nilsga.akka.persistence.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.DynamicMapping.Strict
import com.sksamuel.elastic4s.mappings.FieldType.{BooleanType, LongType, StringType}
import com.sksamuel.elastic4s.mappings.TypedFieldDefinition

import scala.concurrent.Future

object ElasticsearchPersistenceMappings {

  private def ensureIndexAndMappingExists(mappingType: String, mapping: Seq[TypedFieldDefinition])(implicit extension : ElasticsearchPersistenceExtensionImpl) : Future[Unit] = {
    import extension._
    val client = extension.client
    val persistenceIndex = extension.config.index
    val putMapping = put mapping persistenceIndex / mappingType dynamic Strict fields mapping
    client.execute(index exists persistenceIndex).flatMap(r =>
      if (r.isExists) client.execute(putMapping).map(_ => Unit)
      else client.execute(create index persistenceIndex).flatMap(resp => client.execute(putMapping).map(_ => Unit))
    )
  }

  def ensureJournalMappingExists()(implicit extension : ElasticsearchPersistenceExtensionImpl) : Future[Unit] =
    ensureIndexAndMappingExists(extension.config.journalType, Seq(
      field name "persistenceId" withType StringType index NotAnalyzed,
      field name "sequenceNumber" withType LongType,
      field name "message" withType StringType index NotAnalyzed,
      field name "deleted" withType BooleanType
    ))

  def ensureSnapshotMappingExists()(implicit extension : ElasticsearchPersistenceExtensionImpl) : Future[Unit] =
    ensureIndexAndMappingExists(extension.config.snapshotType, Seq(
      field name "persistenceId" withType StringType index NotAnalyzed,
      field name "sequenceNumber" withType LongType,
      field name "timestamp" withType LongType,
      field name "snapshot" withType StringType index NotAnalyzed
    ))

}
