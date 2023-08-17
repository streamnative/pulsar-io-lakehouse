---
dockerfile: ""
alias: Lakehouse Source Connector
---

The Lakehouse source connector (currently only including the [Delta Lake](https://delta.io/) source connector) fetches the Lakehouse table's changelog and saves changelogs into a Pulsar topic.

![](/docs/lakehouse-source.png)

# How to get

This section describes how to build the Lakehouse source connector.

You can get the Lakehouse source connector using one of the following methods:

- Download the NAR package from [the download page](https://github.com/streamnative/pulsar-io-lakehouse/releases).
- Build it from the source code.

To build the Lakehouse source connector from the source code, follow these steps.◊

1. Clone the source code to your machine.

   ```bash
   git clone https://github.com/streamnative/pulsar-io-lakehouse.git
   ```

2. Build the connector in the `pulsar-io-lakehouse` directory.

    - Build the NAR package for your local file system.

        ```bash
        mvn clean install -DskipTests
        ```

    - Build the NAR package for your cloud storage (Including AWS, GCS, and Azure-related package dependency).

        ```bash
        mvn clean install -P cloud -DskipTests
        ```

   After the connector is successfully built, a NAR package is generated under the target directory.

   ```bash
   ls target
   pulsar-io-lakehouse-3.1.0.1.nar
   ```

# How to configure

Before using the Lakehouse source connector, you need to configure it. This table lists the properties and the descriptions.

::: tabs

@@@ Delta Lake
| Name                                 | Type     | Required | Default | Description
|--------------------------------------|----------|----------|---|-------------------------------------------------------------|
| `type` | String | true | N/A | The type of the Lakehouse source connector. Available values: `delta`. |
| `checkpointInterval` | int | false | 30 | The checkpoint interval (in units of seconds). By default, it is set to 30s.  |
| `queueSize` | int | false | 10_000 | The buffer queue size of the Lakehouse source connector. The buffer queue is used for store records before they are sent to Pulsar topics. By default, it is set to `10_000`. |
| `fetchHistoryData` | bool | false | false | Configure whether to fetch the history data of the table. By default, it is set to `false`. |
| `startSnapshotVersion` | long | false | -1 | The Delta snapshot version to start capturing data change. Available values: [-1: LATEST, -2: EARLIEST]. The `startSnapshotVersion` and `startTimestamp` are mutually exclusive.  |
| `startTimestamp` | long | false | N/A | The Delta snapshot timestamp (in units of seconds) to start capturing data change. The `startSnapshotVersion` and `startTimestamp` are mutually exclusive. |
| `tablePath` | String | true | N/A | The path of the Delta table. |
| `parquetParseThreads` | int | false | Runtime.getRuntime().availableProcessors() | The parallelism of paring Delta Parquet files. By default, it is set to `Runtime.getRuntime().availableProcessors()`. |
| `maxReadBytesSizeOneRound` | long | false | Total memory * 0.2 | The maximum read bytes size from Parquet files in one fetch round. By default, it is set to 20% of the heap memory. |
| `maxReadRowCountOneRound` | int | false | 100_000 | The maximum read number of rows processed in one round. By default, it is set to `1_000_000`. |
@@@

:::

> **Note**
>
> The Lakehouse source connector uses the Hadoop file system to read and write data to and from cloud objects, such as AWS, GCS, and Azure. If you want to configure Hadoop related properties, you should use the prefix `hadoop.`.

## Examples

You can create a configuration file (JSON or YAML) to set the properties if you use [Pulsar Function Worker](https://pulsar.apache.org/docs/en/functions-worker/) to run connectors in a cluster.

::: tabs

@@@ Delta Lake

- The Delta table that is stored in the file system

    ```json
    {
        "tenant":"public",
        "namespace":"default",
        "name":"delta_source",
        "parallelism":1,
        "topicName": "delta_source",
        "processingGuarantees":"ATLEAST_ONCE",
        "archive": "connectors/pulsar-io-lakehouse-3.1.0.1.nar",
        "configs":{
            "type":"delta",
            "checkpointInterval": 180,
            "queueSize": 10000,
            "fatchHistoryData": false,
            "startSnapshotVersion": -1,
            "tablePath": "file:///tmp/data/delta-source",
            "parquetParseThreads": 3,
            "maxReadBytesSizeOneRound": 134217728,
            "maxReadRowCountOneRound": 100000
        }
    }
    ```

- The Delta table that is stored in cloud storage (AWS S3, GCS, or Azure)

    ```json
    {
        "tenant":"public",
        "namespace":"default",
        "name":"delta_source",
        "parallelism":1,
        "topicName": "delta_source",
        "processingGuarantees":"ATLEAST_ONCE",
        "archive": "connectors/pulsar-io-lakehouse-3.1.0.1-cloud.nar",
        "configs":{
            "type":"delta",
            "checkpointInterval": 180,
            "queueSize": 10000,
            "fatchHistoryData": false,
            "startSnapshotVersion": -1,
            "tablePath": "s3a://test-dev-us-west-2/lakehouse/delta_source",
            "hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            "parquetParseThreads": 3,
            "maxReadBytesSizeOneRound": 134217728,
            "maxReadRowCountOneRound": 100000
        }
    }
    ```
@@@

:::

## Data format types

Currently, The Lakehouse source connector only supports reading Delta table changelogs, which adopt a `parquet` storage format.


# How to use

You can use the Lakehouse source connector with Function Worker. You can use the Lakehouse source connector as a non built-in connector or a built-in connector.

::: tabs

@@@ Use it as a non built-in connector

If you already have a Pulsar cluster, you can use the Lakehouse source connector as a non built-in connector directly.

This example shows how to create a Lakehouse source connector on a Pulsar cluster using the [`pulsar-admin sources create`](https://pulsar.apache.org/tools/pulsar-admin/2.8.0-SNAPSHOT/#-em-create-em--14) command.

```
PULSAR_HOME/bin/pulsar-admin sources create \
--source-config-file <lakehouse-source-config.yaml>
```

@@@

@@@ Use it as a built-in connector

You can make the Lakehouse source connector as a built-in connector and use it on a standalone cluster or an on-premises cluster.

## Standalone cluster

This example describes how to use the Lakehouse source connector to fetch data from Lakehouse tables and save data to Pulsar topics in standalone mode.

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

3. Run the lakehouse source connector locally.

    ```bash
    PULSAR_HOME/bin/pulsar-admin sources localrun \
    --source-config-file <lakehouse-source-config.yaml>
    ```

4. Write rows into the Lakehouse table. For details, see [Getting Started with Delta Lake](https://delta.io/learn/getting-started).

5. Consume Pulsar topics to get changelogs.

    ```bash
    PULSAR_HOME/bin/pulsar-client consume -s test-sub -n 0 <topic-name>
    ```

## On-premises cluster

This example explains how to create a Lakehouse source connector in an on-premises cluster.

1. Copy the NAR package of the Lakehouse source connector to the Pulsar connectors directory.

    ```
    cp pulsar-io-lakehouse-3.1.0.1.nar $PULSAR_HOME/connectors/pulsar-io-lakehouse-3.1.0.1.nar
    ```

2. Reload all [built-in connectors](https://pulsar.apache.org/docs/en/next/io-connectors/).

    ```
    PULSAR_HOME/bin/pulsar-admin sources reload
    ```

3. Check whether the Lakehouse source connector is available on the list or not.

    ```
    PULSAR_HOME/bin/pulsar-admin sources available-sources
    ```

4. Create a Lakehouse source connector on a Pulsar cluster using the [`pulsar-admin sources create`](https://pulsar.apache.org/tools/pulsar-admin/2.8.0-SNAPSHOT/#-em-create-em--14) command.

    ```
    PULSAR_HOME/bin/pulsar-admin sources create \
    --source-config-file <lakehouse-source-config.yaml>
    ```

@@@

:::

# Demos

This table lists demos that show how to run the [Delta Lake](https://delta.io/), [Hudi](https://hudi.apache.org), and [Iceberg](https://iceberg.apache.org/) source connectors with other external systems.

Currently, only the demo on the Delta Lake source connector is available. 

| Connector  | Link                                                                                                                             |
|------------|----------------------------------------------------------------------------------------------------------------------------------|
| Delta Lake | For details, see the [Delta Lake demo](https://github.com/streamnative/pulsar-io-lakehouse/blob/master/docs/delta-lake-demo.md). |
| Hudi       |                                                                                                                                  |
| Iceberg    |                                                                                                                                  |