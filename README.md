# Elasticsearch distributed journal for Akka persistence (Elastic Search 2.3.3)

Akka persistence distributed journal based on Elasticsearch.

This fork uses Elastic Search version 2.3.3 which is not backward compatible with the original 1.7.1 version.

## Configuration

```
akka.persistence.journal.plugin = "elasticsearch-journal"
akka.persistence.snapshot-store.plugin = "elasticsearch-snapshot-store"

elasticsearch-journal {
  class = "com.github.nilsga.akka.persistence.elasticsearch.ElasticsearchAsyncWriteJournal"
}

elasticsearch-snapshot-store {
  class = "com.github.nilsga.akka.persistence.elasticsearch.ElasticsearchSnapshotStore"
}

elasticsearch-persistence {
  nodes = ["localhost"]
  cluster = "mycluster"
  index = "akkajournal"
}
```

Note: You don't need both the journal and the snapshot store plugin. Just add the plugin that your application is using.

* `elasticsearch-persistence.nodes` is an array of addresses to the ES master nodes of the cluster
* `elasticsearch-persistence.cluster` is the name of the ES cluster to join
* `elasticsearch-persistence.index` is the name of the index to use for the journal

## Why would I use a search engine as a journal?

You probably wouldn't, unless you already have ES as a part of your infrastructure, and don't want to introduce yet another component.

## Versions

This version of `akka-persistence-elasticsearch` requires akka 2.4 and elasticsearch 2.3. It _does not_ work with elasticsearch version 1.7 or older. This versions was only tested with elasticsearch 2.3.3.
