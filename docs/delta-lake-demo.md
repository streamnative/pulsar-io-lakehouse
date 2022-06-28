## Pulsar Delta Lake Connector test Demo

### Test Design
In order to test both the Delta Lake source and sink connector, we design the demo as the following data flow.

![](/docs/delta-lake-demo.png)

1. We use Spark to write rows into the Delta Lake table
2. Deploy Delta Source Connector, monitor the change log of the Delta Lake table, and write the change log into the Pulsar topic
3. Deploy Delta Sink Connector, consume messages from Pulsar topic, and write them into a new Delta Lake table.
4. Use Spark to read the new Delta Lake table, and get all the rows out.

### Run test with local FileSystem
1. Run Pulsar 2.9.1 in standalone mode with Pulsar functions enable.
```bash
bin/pulsar-daemon start standalone
```
2. Get the [pulsar-io-lakehouse](https://github.com/streamnative/pulsar-io-lakehouse/releases) release package, and select version `v2.9.2.22`, named: **pulsar-io-lakehouse-2.9.2.22.nar**

3. Configure Delta Lake Sink connector and submit into Pulsar functions system.
   Delta Lake Sink configuration, `config.json`
```json
{
  "tenant": "public",
  "namespace": "default",
  "name": "delta_sink",
  "parallelism": 1,
  "inputs": [
      "test_delta_pulsar"
  ],
  "archive": "/tmp/lakehouse_connector_test/lake_house_connector/pulsar-io-lakehouse-2.9.2.22.nar",
  "sourceSubscriptionName": "sandbox_sink_v1",
  "processingGuarantees": "EFFECTIVELY_ONCE",
  "configs": {
    "type": "delta",
    "tablePath": "file:///tmp/lakehouse_connector_test/lake_house_connector/data/test_sink_v1",
    "maxCommitInterval": 60,
    "maxRecordsPerCommit": 1_000_000
  }
}
```
Use the following command to submit the sink connector to the Pulsar function system.
```bash
/apache-pulsar-2.9.1/bin/pulsar-admin sinks create \
--sink-config-file /tmp/lakehouse_connector_test/lake_house_connector/delta_sink/config.json \
```
Use the following command to get the list of sink connectors
```bash
bin/pulsar-admin sinks list
```
Use the following command to check the status of the sink connector
```bash
bin/pulsar-admin sinks status --name delta_sink
```

4. Configure Delta Lake Source connector and submit into Pulsar functions system.
   Delta Lake Source configuration, `config.json`
```json
{
  "tenant": "public",
  "namespace": "default",
  "name": "delta_source",
  "topicName": "test_delta_pulsar",
  "parallelism": 1,
  "processingGuarantees": "EFFECTIVELY_ONCE",
  "archive": "/tmp/lakehouse_connector_test/lake_house_connector/pulsar-io-lakehouse-2.9.2.22.nar",
  "configs": {
    "type": "delta",
    "fetchHistoryData": "true",
    "checkpointInterval": 30,
    "tablePath": "/tmp/lakehouse_connector_test/lake_house_connector/data/test_source_v1",
    "maxReadBytesSizeOneRound": 4194304,
    "maxReadRowCountOneRound": 10000
  }
}
```
Use the following command to submit the connector to the Pulsar functions system.
```bash
/apache-pulsar-2.9.1/bin/pulsar-admin sources create \
--source-config-file /tmp/lakehouse_connector_test/lake_house_connector/delta_source/config.json
```
Use the following command to get the list of source connectors
```bash
bin/pulsar-admin sources list
```
Use the following command to check the status of the source connector
```bash
bin/pulsar-admin source status --name delta_source
```
Due to the Delta Lake table not being created yet, the delta source connector will keep restarting until the Delta Lake table is created.

5. Get the [Spark](https://spark.apache.org/downloads.html) binary package(we use `spark-3.2.1-bin-hadoop3.2`), untar it, and use the following command to start it.
```bash
bin/spark-shell --packages io.delta:delta-core_2.12:1.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

6. Use the following command to write data into the Delta Lake table.
```bash
> val sourcePath = "/tmp/lakehouse_connector_test/lake_house_connector/data/test_source_v1"

> for (i <- 0 to 1) {
    spark.range(i * 100, (i + 1) * 100).map(x => (x, x % 5, s"foo-${x % 2}")).toDF("c1", "c2", "c3").write.mode("append").format("delta").save(sourcePath)
}
```

7. Wait 1 minute, and use Spark to query the sink target table. Refer to: https://docs.delta.io/latest/quick-start.html#read-data
```bash
> val sinkPath = "/tmp/lakehouse_connector_test/lake_house_connector/data/test_sink_v1"

> val df = spark.read.format("delta").load(sinkPath)
> df.sort("c1").show(1000)
```

You can go through this [video](https://drive.google.com/file/d/1SRw5Op-qyI7bLTrm39KYnb4kJuddwgzw/view?usp=sharing) to get the details step by step.


### Run tests with S3
1. Run Pulsar 2.9.1 in standalone mode with Pulsar functions enable.
```bash
bin/pulsar-daemon start standalone
```
2. Get the [pulsar-io-lakehouse](https://github.com/streamnative/pulsar-io-lakehouse/releases) release package, and select version `v2.9.2.22`, named: **pulsar-io-lakehouse-2.9.2.22-cloud.nar**

3. Configure Delta Lake Sink connector and submit into Pulsar functions system.
   Delta Lake Sink configuration, `config.json`
```json
{
  "tenant": "public",
  "namespace": "default",
  "name": "delta_sink",
  "parallelism": 1,
  "inputs": [
      "test_delta_pulsar"
  ],
  "archive": "/tmp/lakehouse_connector_test/lake_house_connector/pulsar-io-lakehouse-2.9.2.22-cloud.nar",
  "sourceSubscriptionName": "sandbox_sink_v1",
  "processingGuarantees": "EFFECTIVELY_ONCE",
  "configs": {
    "type": "delta",
    "maxCommitInterval": 60,
    "maxRecordsPerCommit": 1_000_000,
    "tablePath": "s3a://test-dev-us-west-2/lakehouse/delta_sink_v1",
    "hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
  }
}
```
Use the following command to submit the sink connector to the Pulsar function system.
```bash
/tmp/lakehouse_connector_test/apache-pulsar-2.9.1/bin/pulsar-admin sinks create \
--sink-config-file /tmp/lakehouse_connector_test/lake_house_connector/delta_sink/config.json \
```
Use the following command to get the list of sink connectors
```bash
bin/pulsar-admin sinks list
```
Use the following command to check the status of the sink connector
```bash
bin/pulsar-admin sinks status --name delta_sink
```

4. Configure Delta Lake Source connector and submit into Pulsar functions system.
   Delta Lake Source configuration, `config.json`
```json
{
  "tenant": "public",
  "namespace": "default",
  "name": "delta_source",
  "topicName": "test_delta_pulsar",
  "parallelism": 1,
  "processingGuarantees": "EFFECTIVELY_ONCE",
  "archive": "/tmp/lakehouse_connector_test/lake_house_connector/pulsar-io-lakehouse-2.9.2.22.nar",
  "configs": {
    "type": "delta",
    "fetchHistoryData": "true",
    "checkpointInterval": 30,
    "maxReadBytesSizeOneRound": 4194304,
    "maxReadRowCountOneRound": 10000,
    "tablePath": "s3a://test-dev-us-west-2/lakehouse/delta_source_v1",
    "hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"

  }
}
```
Use the following command to submit the connector to the Pulsar functions system.
```bash
/tmp/lakehouse_connector_test/apache-pulsar-2.9.1/bin/pulsar-admin sources create \
--source-config-file /tmp/lakehouse_connector_test/lake_house_connector/delta_source/config.json
```
Use the following command to get the list of source connectors
```bash
bin/pulsar-admin sources list
```
Use the following command to check the status of the source connector
```bash
bin/pulsar-admin source status --name delta_source
```
Due to the Delta Lake table not being created yet, the delta source connector will keep restarting until the Delta Lake table is created.

5. Get the [Spark](https://spark.apache.org/downloads.html) binary package(we use `spark-3.2.1-bin-hadoop3.2`), untar it, and use the following command to start it.
```bash
bin/spark-shell \
 --packages io.delta:delta-core_2.12:1.2.1,org.apache.hadoop:hadoop-aws:3.3.1 \
 --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
 --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_KEY
```

6. Use the following command to write data into the Delta Lake table.
```bash
> val sourcePath = "s3a://test-dev-us-west-2/lakehouse/delta_source_v1"

> for (i <- 0 to 1) {
    spark.range(i * 100, (i + 1) * 100).map(x => (x, x % 5, s"foo-${x % 2}")).toDF("c1", "c2", "c3").write.mode("append").format("delta").save(sourcePath)
}
```

7. Wait 1 minute, and use Spark to query the sink target table. Refer: https://docs.delta.io/latest/quick-start.html#read-data
```bash
> val sinkPath = "s3a://test-dev-us-west-2/lakehouse/delta_sink_v1"

> val df = spark.read.format("delta").load(sinkPath)
> df.sort("c1").show(1000)
```

You can go through this [video](https://drive.google.com/file/d/1pilIZgprEjzSt6QzvgD_7uXNp9fXkD8U/view?usp=sharing) to get the details step by step.

