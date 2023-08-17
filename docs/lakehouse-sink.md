---
dockerfile: ""
alias: Lakehouse Sink Connector
---

The Lakehouse sink connector (including the [Hudi](https://hudi.apache.org), [Iceberg](https://iceberg.apache.org/), and [Delta Lake](https://delta.io/) sink connectors) fetches data from a Pulsar topic and saves data to the Lakehouse tables.

![](/docs/lakehouse-sink.png)

# How to get

This section describes how to build the Lakehouse sink connector.

You can get the Lakehouse sink connector using one of the following methods:

- Download the NAR package from [the download page](https://github.com/streamnative/pulsar-io-lakehouse/releases).
- Build it from the source code.

To build the Lakehouse sink connector from the source code, follow these steps.

1. Clone the source code to your machine.

    ```bash
    git clone https://github.com/streamnative/pulsar-io-lakehouse.git
    ```

2. Build the connector in the `pulsar-io-lakehouse` directory.

   - Build the NAR package for your local file system.

       ```bash
       mvn clean install -DskipTests
       ```

    - Build the NAR package for your cloud storage (Including AWS, GCS and Azure related package dependency).

        ```bash
        mvn clean install -P cloud -DskipTests
        ```
   
   After the connector is successfully built, a NAR package is generated under the target directory.

   ```bash
   ls target
   pulsar-io-lakehouse-3.1.0.1.nar
   ```

# How to configure

Before using the Lakehouse sink connector, you need to configure it. This table lists the properties and the descriptions.

::: tabs

@@@ Hudi
For a list of Hudi configurations, see [Write Client Configs](https://hudi.apache.org/docs/configurations#WRITE_CLIENT).

| Name                                 | Type     | Required | Default | Description
|--------------------------------------|----------|----------|---|-------------------------------------------------------------|
| `type` | String | true | N/A | The type of the Lakehouse source connector. Available values: `hudi`, `iceberg`, and `delta`.         |
| `maxCommitInterval` | Integer | false | 120 | The maximum flush interval (in units of seconds) for each batch. By default, it is set to 120s.                            |
| `maxRecordsPerCommit` | Integer | false | 10_000_000 | The maximum number of records for each batch to commit. By default, it is set to `10_000_000`.                       |
| `maxCommitFailedTimes` | Integer | false | 5 | The maximum commit failure times until failing the process. By default, it is set to `5`.                            |
| `sinkConnectorQueueSize` | Integer | false | 10_000 | The maximum queue size of the Lakehouse sink connector to buffer records before writing to Lakehouse tables. |
| `partitionColumns` | List<String> | false | Collections.empytList() | The partition columns for Lakehouse tables. |                                                   |
| `processingGuarantees` | Int | true | " " (empty string) | The processing guarantees. Currently the Lakehouse connector only supports `EFFECTIVELY_ONCE`. |
| `hudi.table.name`                    | String   | true     | N/A | The name of the Hudi table that Pulsar topic sinks data to.                  |
| `hoodie.table.type`                  | String   | false    | COPY_ON_WRITE | The type of the Hudi table of the underlying data for one write. It cannot be changed between writes. |
| `hoodie.base.path`                   | String   | true     | N/A | The base path of the lake storage where all table data is stored. It always has a specific prefix with the storage scheme (for example, hdfs://, s3:// etc). Hudi stores all the main metadata about commits, savepoints, cleaning audit logs etc in the `.hoodie` directory. |
| `hoodie.datasource.write.recordkey.field` | String | false     | UUID | The record key field. It is used as the `recordKey` component of `HoodieKey`. You can obtain the value by invoking `.toString()` on the field value. You can use the dot notation for nested fields such as a.b.c. |
| `hoodie.datasource.write.partitionpath.field` | String   | true     | N/A | The partition path field. It is used as the `partitionPath` component of the `HoodieKey`.  You can obtain the value by invoking `.toString()`. |
@@@

@@@ Iceberg
| Name                                 | Type     | Required | Default | Description                                                 
|--------------------------------------|----------|----------|---|-------------------------------------------------------------|
| `type` | String | true | N/A | The type of the Lakehouse source connector. Available values: `hudi`, `iceberg`, and `delta`.         |
| `maxCommitInterval` | Integer | false | 120 | The maximum flush interval (in units of seconds) for each batch. By default, it is set to 120s.                            |
| `maxRecordsPerCommit` | Integer | false | 10_000_000 | The maximum number of records for each batch to commit. By default, it is set to `10_000_000`.                       |
| `maxCommitFailedTimes` | Integer | false | 5 | The maximum commit failure times until failing the process. By default, it is set to `5`.                            |
| `sinkConnectorQueueSize` | Integer | false | 10_000 | The maximum queue size of the Lakehouse sink connector to buffer records before writing to Lakehouse tables. |
| `partitionColumns` | List<String> | false | Collections.empytList() | The partition columns for Lakehouse tables. |                                                   |
| `processingGuarantees` | Int | true | " " (empty string) | The processing guarantees. Currently the Lakehouse connector only supports `EFFECTIVELY_ONCE`. |
| `catalogProperties` | Map<String, String> | true | N/A |  The properties of the Iceberg catalog. For details, see  [Iceberg catalog properties](https://iceberg.apache.org/docs/latest/configuration/#catalog-properties). `catalog-impl` and `warehouse` configurations are required. Currently, Iceberg catalogs only support `hadoopCatalog` and `hiveCatalog`. |
| `tableProperties` | Map<String, String> | false | N/A | The properties of the Iceberg table. For details, see [Iceberg  table properties](https://iceberg.apache.org/docs/latest/configuration/#table-properties). |
| `catalogName` | String | false | icebergSinkConnector | The name of the Iceberg catalog. |
| `tableNamespace` | String | true | N/A | The namespace of the Iceberg table. |
| `tableName` | String | true | N/A | The name of the Iceberg table. |
@@@

@@@ Delta Lake
| Name                                 | Type     | Required | Default | Description                                                 
|--------------------------------------|----------|----------|---|-------------------------------------------------------------|
| `type` | String | true | N/A | The type of the Lakehouse source connector. Available values: `hudi`, `iceberg`, and `delta`.         |
| `maxCommitInterval` | Integer | false | 120 | The maximum flush interval (in units of seconds) for each batch. By default, it is set to 120s.                            |
| `maxRecordsPerCommit` | Integer | false | 10_000_000 | The maximum number of records for each batch to commit. By default, it is set to `10_000_000`.                       |
| `maxCommitFailedTimes` | Integer | false | 5 | The maximum commit failure times until failing the process. By default, it is set to `5`.                            |
| `sinkConnectorQueueSize` | Integer | false | 10_000 | The maximum queue size of the Lakehouse sink connector to buffer records before writing to Lakehouse tables. |
| `partitionColumns` | List<String> | false | Collections.empytList() | The partition columns for Lakehouse tables. |                                                   |
| `processingGuarantees` | Int | true | " " (empty string) | The processing guarantees. Currently the Lakehouse connector only supports `EFFECTIVELY_ONCE`. |
| `tablePath` | String | true | N/A | The path of the Delta table. |
| `compression` | String | false | SNAPPY | The compression type of the Delta Parquet file. compression type. By default, it is set to `SNAPPY`. |
| `deltaFileType` | String | false | parquet | The type of the Delta file. By default, it is set to `parquet`. |
| `appId` | String | false | pulsar-delta-sink-connector | The Delta APP ID. By default, it is set to `pulsar-delta-sink-connector`. |
@@@

:::

> **Note**
>
> The Lakehouse sink connector uses the Hadoop file system to read and write data to and from cloud objects, such as AWS, GCS, and Azure. If you want to configure Hadoop related properties, you should use the prefix `hadoop.`.

## Examples

You can create a configuration file (JSON or YAML) to set the properties if you use [Pulsar Function Worker](https://pulsar.apache.org/docs/en/functions-worker/) to run connectors in a cluster.

::: tabs

@@@ Hudi

- The Hudi table that is stored in the file system

   ```json
    {
        "tenant": "public",
        "namespace": "default",
        "name": "hudi-sink",
        "inputs": [
          "test-hudi-pulsar"
        ],
        "archive": "connectors/pulsar-io-hudi-3.1.0.1.nar",
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": 1,
        "configs":   {
            "type": "hudi",
            "hoodie.table.name": "hudi-connector-test",
            "hoodie.table.type": "COPY_ON_WRITE",
            "hoodie.base.path": "file:///tmp/data/hudi-sink",
            "hoodie.datasource.write.recordkey.field": "id",
            "hoodie.datasource.write.partitionpath.field": "id",
        }
    }
   ```

- The Hudi table that is stored in the AWS S3

   ```json
    {
        "tenant": "public",
        "namespace": "default",
        "name": "hudi-sink",
        "inputs": [
          "test-hudi-pulsar"
        ],
        "archive": "connectors/pulsar-io-hudi-3.1.0.1-cloud.nar",
        "parallelism": 1,
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "configs":   {
            "type": "hudi",
            "hoodie.table.name": "hudi-connector-test",
            "hoodie.table.type": "COPY_ON_WRITE",
            "hoodie.base.path": "s3a://bucket/path/to/hudi",
            "hoodie.datasource.write.recordkey.field": "id",
            "hoodie.datasource.write.partitionpath.field": "id",
            "hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        }
    }
   ```
@@@

@@@ Iceberg
- The Iceberg table that is stored in the file system

    ```json
    {
        "tenant":"public",
        "namespace":"default",
        "name":"iceberg_sink",
        "parallelism":2,
        "inputs": [
          "test-iceberg-pulsar"
        ],
        "archive": "connectors/pulsar-io-lakehouse-3.1.0.1.nar",
        "processingGuarantees":"EFFECTIVELY_ONCE",
        "configs":{
            "type":"iceberg",
            "maxCommitInterval":120,
            "maxRecordsPerCommit":10000000,
            "catalogName":"test_v1",
            "tableNamespace":"iceberg_sink_test",
            "tableName":"ice_sink_person",
            "catalogProperties":{
                "warehouse":"file:///tmp/data/iceberg-sink",
                "catalog-impl":"hadoopCatalog"
            }
        }
    }
    ```

- The Iceberg table that is stored in cloud storage (AWS S3, GCS, or Azure)

    ```json
    {
        "tenant":"public",
        "namespace":"default",
        "name":"iceberg_sink",
        "parallelism":2,
        "inputs": [
          "test-iceberg-pulsar"
        ],
        "archive": "connectors/pulsar-io-lakehouse-3.1.0.1-cloud.nar",
        "processingGuarantees":"EFFECTIVELY_ONCE",
        "configs":{
            "type":"iceberg",
            "maxCommitInterval":120,
            "maxRecordsPerCommit":10000000,
            "catalogName":"test_v1",
            "tableNamespace":"iceberg_sink_test",
            "tableName":"ice_sink_person",
            "hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            "catalogProperties":{
                "warehouse":"s3a://test-dev-us-west-2/lakehouse/iceberg_sink",
                "catalog-impl":"hadoopCatalog"
            }
        }
    }
    ```
@@@

@@@ Delta Lake
- The Delta table that is stored in the file system

    ```json
    {
        "tenant":"public",
        "namespace":"default",
        "name":"delta_sink",
        "parallelism":1,
        "inputs": [
          "test-delta-pulsar"
        ],
        "archive": "connectors/pulsar-io-lakehouse-3.1.0.1.nar",
        "processingGuarantees":"EFFECTIVELY_ONCE",
        "configs":{
            "type":"delta",
            "maxCommitInterval":120,
            "maxRecordsPerCommit":10000000,
            "tablePath": "file:///tmp/data/delta-sink"
        }
    }
    ```

- The Delta table that is stored in cloud storage (AWS S3, GCS, or Azure)

    ```json
    {
        "tenant":"public",
        "namespace":"default",
        "name":"delta_sink",
        "parallelism":1,
        "inputs": [
          "test-delta-pulsar"
        ],
        "archive": "connectors/pulsar-io-lakehouse-3.1.0.1-cloud.nar",
        "processingGuarantees":"EFFECTIVELY_ONCE",
        "configs":{
            "type":"delta",
            "maxCommitInterval":120,
            "maxRecordsPerCommit":10000000,
            "tablePath": "s3a://test-dev-us-west-2/lakehouse/delta_sink",
            "hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        }
    }
    ```
@@@

:::

## Data format types

The Lakehouse sink connector provides multiple output format options, including Avro and Parquet. The default format is Parquet.
With the current implementation, there are some limitations for different formats:

This table lists the Pulsar Schema types supported by the writers.

| Pulsar Schema    | Writer: Avro | Writer: Parquet |
|------------------|--------------|-----------------|
| Primitive        | ✗            | ✗               |
| Avro             | ✔            | ✔               |
| Json             | ✔            | ✔               |
| Protobuf *       | ✗            | ✗               |
| ProtobufNative * | ✗            | ✗               |

> *: The Protobuf schema is based on the Avro schema. It uses Avro as an intermediate format, so it may not provide the best effort conversion.
>
> *: The ProtobufNative record holds the Protobuf descriptor and the message. When writing to Avro format, the connector uses [avro-protobuf](https://github.com/apache/avro/tree/master/lang/java/protobuf) to do the conversion.

# How to use

You can use the Lakehouse sink connector with Function Worker. You can use the Lakehouse sink connector as a non built-in connector or a built-in connector.

::: tabs

@@@ Use it as a non built-in connector

If you already have a Pulsar cluster, you can use the Lakehouse sink connector as a non built-in connector directly.

This example shows how to create a Lakehouse sink connector on a Pulsar cluster using the [`pulsar-admin sinks create`](http://pulsar.apache.org/tools/pulsar-admin/2.8.0-SNAPSHOT/#-em-create-em--24) command.

```
PULSAR_HOME/bin/pulsar-admin sinks create \
--sink-config-file <lakehouse-sink-config.yaml>
```

@@@

@@@ Use it as a built-in connector

You can make the Lakehouse sink connector as a built-in connector and use it on a standalone cluster or an on-premises cluster.

## Standalone cluster

This example describes how to use the Lakehouse sink connector to fetch data from Pulsar topics and save data to Lakehouse tables in standalone mode.

### Prerequisites

- Install Pulsar locally. For details, see [set up a standalone Pulsar locally](https://pulsar.apache.org/docs/en/standalone/#install-pulsar-using-binary-release).

### Steps

1. Copy the NAR package to the Pulsar connectors directory.

    ```
    cp pulsar-io-lakehouse-3.1.0.1.nar PULSAR_HOME/connectors/pulsar-io-lakehouse-3.1.0.1.nar
    ```

2. Start Pulsar in standalone mode.

    ```
    PULSAR_HOME/bin/pulsar standalone
    ```

3. Run the lakehouse sink connector locally.

    ```
    PULSAR_HOME/bin/pulsar-admin sink localrun \
    --sink-config-file <lakehouse-sink-config.yaml>
    ```

4. Send messages to Pulsar topics.

   This example sends ten “hello” messages to the `test-lakehouse-pulsar` topic in the `default` namespace of the `public` tenant.

    ```
    PULSAR_HOME/bin/pulsar-client produce public/default/test-lakehouse-pulsar --messages hello -n 10
    ```

5. Query the data from the Lakehouse table. For details, see [Hudi Quickstart guide](https://hudi.apache.org/docs/quick-start-guide), [Iceberg Quickstart guide](https://iceberg.apache.org/docs/latest/getting-started/), and [Delta Quickstart guide](https://delta.io/learn/getting-started).

## On-premises cluster

This example explains how to create a Lakehouse sink connector in an on-premises cluster.

1. Copy the NAR package of the Lakehouse sink connector to the Pulsar connectors directory.

    ```bash
    cp pulsar-io-lakehouse-3.1.0.1.nar $PULSAR_HOME/connectors/pulsar-io-lakehouse-3.1.0.1.nar
    ```

2. Reload all [built-in connectors](https://pulsar.apache.org/docs/en/next/io-connectors/).

    ```bash
    PULSAR_HOME/bin/pulsar-admin sinks reload
    ```

3. Check whether the Lakehouse sink connector is available on the list or not.

    ```bash
    PULSAR_HOME/bin/pulsar-admin sinks available-sinks
    ```

4. Create a Lakehouse sink connector on a Pulsar cluster using the [`pulsar-admin sinks create`](http://pulsar.apache.org/tools/pulsar-admin/2.8.0-SNAPSHOT/#-em-create-em--24) command.

    ```bash 
    PULSAR_HOME/bin/pulsar-admin sinks create \
    --sink-config-file <lakehouse-sink-config.yaml>
    ```

@@@

:::

# Demos

This table lists demos that show how to run the [Delta Lake](https://delta.io/), [Hudi](https://hudi.apache.org), and [Iceberg](https://iceberg.apache.org/) sink connectors with other external systems.

Currently, only the demo on the Delta Lake sink connector is available. 

| Connector  | Link                                                                                                                             |
|------------|----------------------------------------------------------------------------------------------------------------------------------|
| Delta Lake | For details, see the [Delta Lake demo](https://github.com/streamnative/pulsar-io-lakehouse/blob/master/docs/delta-lake-demo.md). |
| Hudi       |                                                                                                                                  |
| Iceberg    |                                                                                                                                  |
