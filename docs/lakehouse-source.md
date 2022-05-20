The Lakehouse (currently only including [DeltaLake](https://delta.io/))  connector fetch lakehouse table's changelog and save changelogs into Pulsar topic..

![](/docs/lakehouse-source.png)

# How to get
This section describes how to build the Lakehouse source connector.

You can get the Lakehouse source connector using one of the following methods

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

Before using the Lakehouse source connector, you need to configure it. This table lists the properties and the descriptions.

Common Configuration

| Name                                 | Type     | Required | Default | Description
|--------------------------------------|----------|----------|---------|-------------------------------------------------------------|
| type | String | true | N/A | The type of lakehouse connector. Available values: `delta` |
| checkpointInterval | int | false | 30 | Checkpoint interval. TimeUnit: second. Default is 30s |
| queueSize | int | false | 10_000 | Source connector buffer queue size, used for store records before send to pulsar topic. Default is 10_000 |
| fetchHistoryData | bool | false | false | Whether fetch the history data of the table. Default is `false` |
| startSnapshotVersion | long | false | -1 | Delta snapshot version to start to capture data change. Values: [-1: LATEST, -2: EARLIEST]. The `startSnapshotVersion` and `startTimestamp` can only configure one |
| startTimestamp | long | false |  | Delta snapshot timestamp to start to capture data change, Time unit: second. The startSnapshotVersion and startTimestamp can only configure one |

Lakehouse specific configuration

::: tabs

@@@ DeltaLake Configuration

| Name                                 | Type     | Required | Default | Description
|--------------------------------------|----------|----------|---|-------------------------------------------------------------|
| tablePath | String | true | N/A | Delta lake table path |
| parquetParseThreads | int | false | Runtime.getRuntime().availableProcessors() | The parallelism of paring delta parquet files. Default is `Runtime.getRuntime().availableProcessors()` |
| maxReadBytesSizeOneRound | long | false | total memory * 0.2	| The max read bytes size from parquet files in one fetch round. Default is 20% of heap memory |
| maxReadRowCountOneRound | int | false | 100_000 | The max read number of rows process in one round. Default is 1_000_000 |


This Lakehouse connector use hadoop file system to read and write cloud object, such as `aws`, `gcs` and `azure`. If we want to configure hadoop cloud related properties, we should start the prefix `hadoop.`

## Configure with Function Worker

You can create a configuration file (JSON or YAML) to set the properties if you use [Pulsar Function Worker](https://pulsar.apache.org/docs/en/functions-worker/) to run connectors in a cluster.

**Example**

::: tabs

@@@ DeltaLake Example

DeltaLake table stored in file system

```json
{
    "tenant":"public",
    "namespace":"default",
    "name":"delta_source",
    "parallelism":1,
    "topicName": "delta_source",
    "processingGuarantees":"ATLEAST_ONCE",
    "archive": "connectors/pulsar-io-lakehouse-{{connector:version}}.nar",
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

DetlaLake table stored in cloud storage(s3, gcs or azure)

```json
{
    "tenant":"public",
    "namespace":"default",
    "name":"delta_source",
    "parallelism":1,
    "topicName": "delta_source",
    "processingGuarantees":"ATLEAST_ONCE",
    "archive": "connectors/pulsar-io-lakehouse-{{connector:version}}.nar",
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

### Data format types

Currently, The Lakehouse Source Connector only support read delta lake table changelogs, whose storage formate is `parquet`


## Configure with Function Mesh

TBD

# How to use

You can use the Lakehouse source connector with Function Worker or Function Mesh.

## Work with Function Worker

You can use the Lakehouse source connector as a non built-in connector or a built-in connector.

::: tabs

@@@ Use it as non built-in connector

If you already have a Pulsar cluster, you can use the Lakehouse source connector as a non built-in connector directly.

This example shows how to create a Lakehouse source connector on a Pulsar cluster using the [`pulsar-admin sinks create`](http://pulsar.apache.org/tools/pulsar-admin/2.8.0-SNAPSHOT/#-em-create-em--24) command.

```
$ PULSAR_HOME/bin/pulsar-admin sources create \
--source-config-file <lakehouse-source-config.yaml>
```

@@@

@@@ Use it as built-in connector

You can make the Lakehouse source connector as a built-in connector and use it on a standalone cluster or an on-premises cluster.

### Standalone cluster

This example describes how to use the Lakehouse source connector to fetch data from Pulsar topics and save data to lakehouse tables in standalone mode.

#### Prerequisites

- Install Pulsar locally. For details, see [set up a standalone Pulsar locally](https://pulsar.apache.org/docs/en/standalone/#install-pulsar-using-binary-release).

#### Steps

1. Copy the NAR package to the Pulsar connectors directory.

    ```
    cp pulsar-io-lakehouse-{{connector:version}}.nar PULSAR_HOME/connectors/pulsar-io-lakehouse-{{connector:version}}.nar
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

4. Write rows into Lakehouse table. You can follow this guide [delta](https://delta.io/learn/getting-started)

5. Consume pulsar topics to get changelogs.
    ```bash
    $ PULSAR_HOME/bin/pulsar-client consume -s test-sub -n 0 <topic-name>
    ```

### On-premises cluster

This example explains how to create a Lakehouse source connector in an on-premises cluster.

1. Copy the NAR package of the Lakehouse source connector to the Pulsar connectors directory.

    ```
    cp pulsar-io-lakehouse-{{connector:version}}.nar $PULSAR_HOME/connectors/pulsar-io-lakehouse-{{connector:version}}.nar
    ```

2. Reload all [built-in connectors](https://pulsar.apache.org/docs/en/next/io-connectors/).

    ```
    PULSAR_HOME/bin/pulsar-admin sources reload
    ```

3. Check whether the Lakehouse source connector is available on the list or not.

    ```
    PULSAR_HOME/bin/pulsar-admin sources available-sinks
    ```

4. Create a Lakehouse source connector on a Pulsar cluster using the [`pulsar-admin sources create`](https://pulsar.apache.org/tools/pulsar-admin/2.8.0-SNAPSHOT/#-em-create-em--14) command.

    ```
    PULSAR_HOME/bin/pulsar-admin sources create \
    --source-config-file <lakehouse-source-config.yaml>
    ```

@@@

:::

## Work with Function Mesh

TBD
