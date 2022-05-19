The Lakehouse (including [Iceberg](https://iceberg.apache.org/), [Hudi](https://hudi.apache.org) and [DeltaLake](https://delta.io/)) sink connector fetch data from Pulsar topics and save data
to Lakehouse tables.

![](/docs/lakehouse-sink.png)

# How to get
This section describes how to build the Lakehouse sink connector.

You can get the Lakehouse sink connector using one of the following methods

If you use [Pulsar Function Worker](https://pulsar.apache.org/docs/en/functions-worker/) to run connectors in a cluster.
    - Download the NAR package from [the download page](https://github.com/streamnative/pulsar-io-lakehouse/releases).
    - Build it from the source code.

To build the Lakehouse sink connector from the source code, follow these steps.
1. Clone the source code to your machin.
   ```bash
   $ git clone https://github.com/streamnative/pulsar-io-lakehouse.git
   ```

2. Build the connector in the `pulsar-io-lakehouse` directory.
   - Build local file system NAR package
       ```bash
       $ mvn clean install -DskipTests
       ```

   - Build cloud NAR package (Including aws, gcs and azure related package dependency)
       ```bash
       $ mvn clean install -P cloud -DskipTests
       ```
   
   After the connector is successfully built, a `NAR` package is generated under the target directory.
   ```bash
   $ ls target
   pulsar-io-lakehouse-{{connector:version}}.nar
   ```

# How to configure

Before using the Lakehouse sink connector, you need to configure it. This table lists the properties and the descriptions.

Common Configuration

| Name                                 | Type     | Required | Default | Description                                                                              
|--------------------------------------|----------|----------|---------|------------------------------------------------------------------------------------------|
| className | String | true | N/A | Sink connector className.  Should be `org.apache.pulsar.ecosystem.io.SinkConnector`      |
| type | String | true | N/A | The type of lakehouse connector. Available values: `hudi`, `iceberg` and `delta`         |
| maxCommitInterval | Integer | false | 120 | Max flush interval in seconds for each batch. Default is 120s                            |
| maxRecordsPerCommit | Integer | false | 10_000_000 | Max records number for each batch to commit. Default is 10_000_000                       |
| maxCommitFailedTimes | Integer | false | 5 | Max commit fail times until failing the process. Default is 5                            |
| sinkConnectorQueueSize | Integer | false | 10_000 | The max queue size of sink connector to buffer records before writing to lakehouse table |
| partitionColumns | List<String> | false | Collections.empytList() | Partition columns for lakehouse table                                                    |

`processingGuarantees`: Currently only support `EFFECTIVELY_ONCE`

Lakehouse specific configuration

::: tabs

@@@ Hudi configuration

For the Hudi configurations, you can use all the configs list in [here](https://hudi.apache.org/docs/configurations#WRITE_CLIENT) to configure the Hudi write client.

| Name                                 | Type     | Required | Default | Description
|--------------------------------------|----------|----------|---|-------------------------------------------------------------|
| `hudi.table.name`                    | String   | true     | N/A | The table name that Pulsar topic sinks to.                  |
| `hoodie.table.type`                  | String   | false    | COPY_ON_WRITE | The table type for the underlying data, for this write. This can’t change between writes. |
| `hoodie.base.path`                   | String   | true     | N/A | Base path on lake storage, under which all the table data is stored. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory. |
| `hoodie.datasource.write.recordkey.field` | String | false     | uuid | Record key field. Value to be used as the recordKey component of HoodieKey. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: a.b.c. |
| `hoodie.datasource.write.partitionpath.field` | String   | true     | N/A | Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value ontained by invoking .toString(). |
| `connector.hoodieCommitMaxRecords`   | Integer  | false    | 1000 | The max records received from pulsar before doing a commit. |
| `connector.hoodieCommitIntervalSecs` | Integer  | false    | 60 | The max time interval between the commit operation.         |


@@@ Iceberg Configuration

| Name                                 | Type     | Required | Default | Description                                                 
|--------------------------------------|----------|----------|---|-------------------------------------------------------------|
| catalogProperties | Map<String, String> | true | N/A |  Refer to [iceberg catalog-properties](https://iceberg.apache.org/docs/latest/configuration/#catalog-properties). `catalog-impl` and `warehouse` configuration is required. Only support `hadoopCatalog` and `hiveCatalog` |
| tableProperties | Map<String, String> | false | N/A | Refer to [iceberg table-properties](https://iceberg.apache.org/docs/latest/configuration/#table-properties). |
| catalogName | String | false | icebergSinkConnector | Iceberg catalog name |
| tableNamespace | String | true | N/A | Iceberg table namespace |
| tableName | String | true | N/A | Icberg table name |

@@@ DeltaLake Configuration

| Name                                 | Type     | Required | Default | Description                                                 
|--------------------------------------|----------|----------|---|-------------------------------------------------------------|
| tablePath | String | true | N/A | Delta lake table path |
| compression | String | false | SNAPPY | Delta lake parquet file compression type. Default is `SNAPPY` |
| deltaFileType | String | false | parquet | Delta lake file type. Default is `parquet` |
| appId | String | false | pulsar-delta-sink-connector | Delta lake appId. Default is `pulsar-delta-sink-connector` |

This Lakehouse connector use hadoop file system to read and write cloud object, such as `aws`, `gcs` and `azure`. If we want to configure hadoop cloud related properties, we should start the prefix `hadoop.`


## Configure with Function Worker

You can create a configuration file (JSON or YAML) to set the properties if you use [Pulsar Function Worker](https://pulsar.apache.org/docs/en/functions-worker/) to run connectors in a cluster.

**Example**

::: tabs

@@@ Hudi Example

* JSON

   ```json
    {
        "tenant": "public",
        "namespace": "default",
        "name": "hudi-sink",
        "inputs": [
          "test-hudi-pulsar"
        ],
        "archive": "connectors/pulsar-io-hudi-{{connector:version}}.nar",
        "parallelism": 1,
        "className": "org.apache.pulsar.ecosystem.io.SinkConnector",
        "configs":   {
            "hoodie.table.name": "hudi-connector-test",
            "hoodie.table.type": "COPY_ON_WRITE",
            "hoodie.base.path": "file:///tmp/data/hudi-sink",
            "hoodie.datasource.write.recordkey.field": "id",
            "hoodie.datasource.write.partitionpath.field": "id",
            "connector.hoodieCommitMaxRecords": 1000,
            "connector.hoodieCommitIntervalSecs": 60
        }
    }
    ```

* YAML

    ```yaml
    tenant: public
    namespace: default
    name: hudi-sink
    inputs:
      - test-hudi-pulsar
    archive: connectors/pulsar-io-hudi-{{connector:version}}.nar
    parallelism: 1
    className: org.apache.pulsar.ecosystem.io.SinkConnector
    configs:
      hoodie.table.name: hudi-connector-test
      hoodie.table.type: COPY_ON_WRITE
      hoodie.base.path: file:///tmp/data/hudi-sink
      hoodie.datasource.write.recordkey.field: id
      hoodie.datasource.write.partitionpath.field: id
      connector.hoodieCommitMaxRecords: 1000
      connector.hoodieCommitIntervalSecs: 60
    ```

@@@ Iceberg Example

Iceberg table stored in file system

```json
{
    "tenant":"public",
    "namespace":"default",
    "name":"iceberg_sink",
    "parallelism":2,
    "inputs": [
      "test-iceberg-pulsar"
    ],
    "archive": "connectors/pulsar-io-lakehouse-{{connector:version}}.nar",
    "processingGuarantees":"EFFECTIVELY_ONCE",
    "className":"org.apache.pulsar.ecosystem.io.SinkConnector",
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

Iceberg table stored in cloud storage(s3, gcs or azure)

```json
{
    "tenant":"public",
    "namespace":"default",
    "name":"iceberg_sink",
    "parallelism":2,
    "inputs": [
      "test-iceberg-pulsar"
    ],
    "archive": "connectors/pulsar-io-lakehouse-{{connector:version}}.nar",
    "processingGuarantees":"EFFECTIVELY_ONCE",
    "className":"org.apache.pulsar.ecosystem.io.SinkConnector",
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

@@@ DeltaLake Example

DeltaLake table stored in file system

```json
{
    "tenant":"public",
    "namespace":"default",
    "name":"delta_sink",
    "parallelism":1,
    "inputs": [
      "test-delta-pulsar"
    ],
    "archive": "connectors/pulsar-io-lakehouse-{{connector:version}}.nar",
    "processingGuarantees":"EFFECTIVELY_ONCE",
    "className":"org.apache.pulsar.ecosystem.io.SinkConnector",
    "configs":{
        "type":"delta",
        "maxCommitInterval":120,
        "maxRecordsPerCommit":10000000,
        "tablePath": "file:///tmp/data/delta-sink"
    }
}
```

Iceberg table stored in cloud storage(s3, gcs or azure)

```json
{
    "tenant":"public",
    "namespace":"default",
    "name":"delta_sink",
    "parallelism":1,
    "inputs": [
      "test-delta-pulsar"
    ],
    "archive": "connectors/pulsar-io-lakehouse-{{connector:version}}.nar",
    "processingGuarantees":"EFFECTIVELY_ONCE",
    "className":"org.apache.pulsar.ecosystem.io.SinkConnector",
    "configs":{
        "type":"delta",
        "maxCommitInterval":120,
        "maxRecordsPerCommit":10000000,
        "tablePath": "s3a://test-dev-us-west-2/lakehouse/delta_sink",
        "hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }
}
```

### Data format types

Lakehouse Sink Connector provides multiple output format options, including Avro and Parquet. The default format is Parquet.
With current implementation, there are some limitations for different formats:

This table lists the Pulsar Schema types supported by the writers.

| Pulsar Schema  | Writer: Avro | Writer: Parquet |
|----------------|--------------|-----------------|
| Primitive      | ✗            | ✗               |
| Avro           | ✔            | ✔               |
| Json           | ✔            | ✔               |
| Protobuf *     | ✗            | ✗               |
| ProtobufNative * | ✗            | ✗               | 
> *: The Protobuf schema is based on the Avro schema. It uses Avro as an intermediate format, so it may not provide the best effort conversion.
>
> *: The ProtobufNative record holds the Protobuf descriptor and the message. When writing to Avro format, the connector uses [avro-protobuf](https://github.com/apache/avro/tree/master/lang/java/protobuf) to do the conversion.

## Configure with Function Mesh

TBD

# How to use

You can use the Lakehouse sink connector with Function Worker or Function Mesh.

## Work with Function Worker

You can use the Lakehouse sink connector as a non built-in connector or a built-in connector.

::: tabs

@@@ Use it as non built-in connector

If you already have a Pulsar cluster, you can use the Lakehouse sink connector as a non built-in connector directly.

This example shows how to create a Lakehouse sink connector on a Pulsar cluster using the [`pulsar-admin sinks create`](http://pulsar.apache.org/tools/pulsar-admin/2.8.0-SNAPSHOT/#-em-create-em--24) command.

```
$ PULSAR_HOME/bin/pulsar-admin sinks create \
--sink-config-file <lakehouse-sink-config.yaml>
```

@@@

@@@ Use it as built-in connector

You can make the Lakehouse sink connector as a built-in connector and use it on a standalone cluster or an on-premises cluster.

### Standalone cluster

This example describes how to use the Lakehouse sink connector to fetch data from Pulsar topics and save data to Lakehouse tables in standalone mode.

#### Prerequisites

- Install Pulsar locally. For details, see [set up a standalone Pulsar locally](https://pulsar.apache.org/docs/en/standalone/#install-pulsar-using-binary-release).

#### Steps

1. Copy the NAR package to the Pulsar connectors directory.

    ```
    $ cp pulsar-io-lakehouse-{{connector:version}}.nar PULSAR_HOME/connectors/pulsar-io-lakehouse-{{connector:version}}.nar
    ```

2. Start Pulsar in standalone mode.

    ```
    $ PULSAR_HOME/bin/pulsar standalone
    ```

3. Run the lakehouse sink connector locally.

    ```
    $ PULSAR_HOME/bin/pulsar-admin sink localrun \
    --sink-config-file <lakehouse-sink-config.yaml>
    ```

4. Send messages to Pulsar topics.

   This example sends ten “hello” messages to the `test-lakehouse-pulsar` topic in the `default` namespace of the `public` tenant.

    ```
    $ PULSAR_HOME/bin/pulsar-client produce public/default/test-lakehouse-pulsar --messages hello -n 10
    ```

5. Query the data from the Lakehouse table. you can follow this guide [hudi](https://hudi.apache.org/docs/quick-start-guide), [iceberg](https://iceberg.apache.org/docs/latest/getting-started/) and [delta](https://delta.io/learn/getting-started) to query the data

### On-premises cluster

This example explains how to create a Lakehouse sink connector in an on-premises cluster.

1. Copy the NAR package of the Lakehouse sink connector to the Pulsar connectors directory.

    ```bash
    $ cp pulsar-io-lakehouse-{{connector:version}}.nar $PULSAR_HOME/connectors/pulsar-io-lakehouse-{{connector:version}}.nar
    ```

2. Reload all [built-in connectors](https://pulsar.apache.org/docs/en/next/io-connectors/).

    ```bash
    $ PULSAR_HOME/bin/pulsar-admin sinks reload
    ```

3. Check whether the Lakehouse sink connector is available on the list or not.

    ```bash
    $ PULSAR_HOME/bin/pulsar-admin sinks available-sinks
    ```

4. Create a Lakehouse sink connector on a Pulsar cluster using the [`pulsar-admin sinks create`](http://pulsar.apache.org/tools/pulsar-admin/2.8.0-SNAPSHOT/#-em-create-em--24) command.

    ```bash 
    $ PULSAR_HOME/bin/pulsar-admin sinks create \
    --sink-config-file <lakehouse-sink-config.yaml>
    ```

@@@

:::

## Work with Function Mesh

TBD
