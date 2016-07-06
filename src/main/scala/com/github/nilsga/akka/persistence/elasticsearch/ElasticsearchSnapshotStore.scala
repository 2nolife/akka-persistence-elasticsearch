package com.github.nilsga.akka.persistence.elasticsearch

import akka.persistence.serialization.Snapshot
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import akka.serialization.SerializationExtension
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.RequestBuilder
import com.sksamuel.elastic4s.{BulkCompatibleDefinition, RichSearchHit}
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.common.Base64
import org.elasticsearch.search.sort.SortOrder

import scala.collection.JavaConversions
import scala.concurrent.{Future, Promise}

class ElasticsearchSnapshotStore extends SnapshotStore {
  import context._

  implicit val extension = ElasticsearchPersistenceExtension(system)
  val serializer = SerializationExtension(system)
  val esClient = extension.client
  val persistenceIndex = extension.config.index
  val snapshotType = extension.config.snapshotType

  implicit private def long2string(x: Long): String = x.toString

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    esClient.execute(refresh index persistenceIndex).flatMap(_ => {
      val query = search in persistenceIndex / snapshotType query {
        must(
          termQuery("persistenceId", persistenceId),
          rangeQuery("sequenceNumber") gte criteria.minSequenceNr lte criteria.maxSequenceNr,
          rangeQuery("timestamp") gte criteria.minTimestamp lte criteria.maxTimestamp
        )
      } sourceInclude "_id" sort (field sort "timestamp" order SortOrder.DESC)

      esClient.execute(query).flatMap(searchResponse => {
        val gets = searchResponse.hits.map(get id _.id from persistenceIndex / snapshotType)
        esClient.execute(multiget(gets))
      }).map(multiGetResponse =>
        multiGetResponse.responses.collectFirst { case r if !r.isFailed => toSelectedSnapshot(r.response.get) }
      )
    })
  }

  private def toSelectedSnapshot(response : GetResponse): SelectedSnapshot = {
    val source = response.getSourceAsMap
    val persistenceId = source.get("persistenceId").asInstanceOf[String]
    val sequenceNr = source.get("sequenceNumber").asInstanceOf[Number].longValue
    val timestamp = source.get("timestamp").asInstanceOf[Number].longValue
    val snapshotB64 = source.get("snapshot").asInstanceOf[String]
    val snapshot = serializer.deserialize[Snapshot](Base64.decode(snapshotB64), classOf[Snapshot]).get.data
    SelectedSnapshot(SnapshotMetadata(persistenceId, sequenceNr, timestamp), snapshot)
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val snapshotId = s"${metadata.persistenceId}-${metadata.sequenceNr}"

    esClient.execute(update id snapshotId in persistenceIndex / snapshotType docAsUpsert(
      "persistenceId" -> metadata.persistenceId,
      "sequenceNumber" -> metadata.sequenceNr,
      "timestamp" -> metadata.timestamp,
      "snapshot" -> serializer.serialize(Snapshot(snapshot)).get
      )
    ).map(_ => Unit)
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val snapshotId = s"${metadata.persistenceId}-${metadata.sequenceNr}"
    esClient.execute(delete id snapshotId from persistenceIndex / snapshotType).map(_ => Unit)
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val snapshotsToDelete = esClient.publisher(search in persistenceIndex / snapshotType sourceInclude "_id" query {
      must(
        termQuery("persistenceId", persistenceId),
        rangeQuery("sequenceNumber") gte criteria.minSequenceNr lte criteria.maxSequenceNr,
        rangeQuery("timestamp") gte criteria.minTimestamp lte criteria.maxTimestamp
      )
    } scroll "1m")

    val reqBuilder = new RequestBuilder[RichSearchHit] {
      override def request(t: RichSearchHit): BulkCompatibleDefinition =
        delete id t.id from persistenceIndex / snapshotType
    }

    val promise = Promise[Unit]

    val subscriber = esClient.subscriber(
      100, 1,
      completionFn = () => promise.success((): Unit),
      errorFn = (ex) => promise.failure(ex)
    )(reqBuilder, context.system)

    snapshotsToDelete.subscribe(subscriber)

    promise.future
  }
}
