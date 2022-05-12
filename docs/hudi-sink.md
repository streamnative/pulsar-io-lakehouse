The [Hudi](https://hudi.apache.org/) sink connector fetch data from Pulsar topics and save data
to Hudi tables.

![](/docs/hudi-sink.png)

# How to get
This section describes how to build the Hudi sink connector.

## Work with Function Worker

You can get the Hudi sink connector using one of the following methods if you use [Pulsar Function Worker](https://pulsar.apache.org/docs/en/functions-worker/)
to run connectors in a cluster.

- Download the NAR package from [the download page](https://github.com/streamnative/pulsar-io-hudi/releases).
- Build it from the source code.

To build the Hudi sink connector from the source code, follow these steps.

1. Clone the source code to your machin.
   ```bash
   git clone https://github.com/streamnative/pulsar-io-hudi.git
   ```
   
2. Build the connector in the `pulsar-io-hudi` directory.
   ```bash
   mvn clean install -DskipTests
   ```
   After the connector is successfully built, a `NAR` package is generated under the target directory.
   ```bash
   ls target
   pulsar-io-hudi-{{connector:version}}.nar
   ```

## Work with Function Mesh

TBD

# How to configure

Before using the Hudi sink connector, you need to configure it. This table lists the properties and the descriptions.
For the Hudi configurations, you can use all the configs list in [here](https://hudi.apache.org/docs/configurations#WRITE_CLIENT)
to configure the Hudi write client.

| Name                                 | Type     | Required | Default | Description                                                 
|--------------------------------------|----------|----------|--|-------------------------------------------------------------|
| `hudi.table.name`                    | String   | true     | N/A | The table name that Pulsar topic sinks to.                  |
| `hoodie.table.type`                  | String   | false    | COPY_ON_WRITE | The table type for the underlying data, for this write. This can’t change between writes. |
| `hoodie.base.path`                   | String   | true     | N/A | Base path on lake storage, under which all the table data is stored. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory. |
| `hoodie.datasource.write.recordkey.field` | String | false     | uuid | Record key field. Value to be used as the recordKey component of HoodieKey. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: a.b.c. |
| `hoodie.datasource.write.partitionpath.field` | String   | true     | N/A | Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value ontained by invoking .toString(). |
| `connector.hoodieCommitMaxRecords`   | Integer  | false    | 1000 | The max records received from pulsar before doing a commit. |
| `connector.hoodieCommitIntervalSecs` | Integer  | false    | 60 | The max time interval between the commit operation.         |


## Work with Function Worker

You can create a configuration file (JSON or YAML) to set the properties if you use [Pulsar Function Worker](https://pulsar.apache.org/docs/en/functions-worker/) to run connectors in a cluster.

**Example**

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

## Work with Function Mesh

TBD

# How to use

You can use the Hudi sink connector with Function Worker or Function Mesh.

## Work with Function Worker

You can use the Hudi sink connector as a non built-in connector or a built-in connector.

::: tabs

@@@ Use it as non built-in connector

If you already have a Pulsar cluster, you can use the Hudi sink connector as a non built-in connector directly.

This example shows how to create a Hudi sink connector on a Pulsar cluster using the [`pulsar-admin sinks create`](http://pulsar.apache.org/tools/pulsar-admin/2.8.0-SNAPSHOT/#-em-create-em--24) command.

```
PULSAR_HOME/bin/pulsar-admin sinks create \
--sink-config-file <hudi-sink-config.yaml>
```

@@@

@@@ Use it as built-in connector

You can make the Hudi sink connector as a built-in connector and use it on a standalone cluster or an on-premises cluster.

### Standalone cluster

This example describes how to use the Hudi sink connector to fetch data from Pulsar topics and save data to Hudi tables in standalone mode.

#### Prerequisites

- Install Pulsar locally. For details, see [set up a standalone Pulsar locally](https://pulsar.apache.org/docs/en/standalone/#install-pulsar-using-binary-release).

#### Steps

1. Copy the NAR package to the Pulsar connectors directory.

    ```
    cp pulsar-io-hudi-{{connector:version}}.nar PULSAR_HOME/connectors/pulsar-io-hudi-{{connector:version}}.nar
    ```

2. Start Pulsar in standalone mode.

    ```
    PULSAR_HOME/bin/pulsar standalone
    ```

3. Run the hudi sink connector locally.

    ```
    PULSAR_HOME/bin/pulsar-admin sink localrun \
    --sink-config-file <hudi-sink-config.yaml>
    ```

4. Send messages to Pulsar topics.

   This example sends ten “hello” messages to the `test-hudi-pulsar` topic in the `default` namespace of the `public` tenant.

    ```
    PULSAR_HOME/bin/pulsar-client produce public/default/test-hudi-pulsar --messages hello -n 10
    ```

5. Query the data from the Hudi table. You can follow [this guide](https://hudi.apache.org/docs/quick-start-guide) to query the data through Spark.

### On-premises cluster

This example explains how to create a Hudi sink connector in an on-premises cluster.

1. Copy the NAR package of the Hudi sink connector to the Pulsar connectors directory.

    ```
    cp pulsar-io-hudi-{{connector:version}}.nar $PULSAR_HOME/connectors/pulsar-io-hudi-{{connector:version}}.nar
    ```

2. Reload all [built-in connectors](https://pulsar.apache.org/docs/en/next/io-connectors/).

    ```
    PULSAR_HOME/bin/pulsar-admin sinks reload
    ```

3. Check whether the Hudi sink connector is available on the list or not.

    ```
    PULSAR_HOME/bin/pulsar-admin sinks available-sinks
    ```

4. Create a Hudi sink connector on a Pulsar cluster using the [`pulsar-admin sinks create`](http://pulsar.apache.org/tools/pulsar-admin/2.8.0-SNAPSHOT/#-em-create-em--24) command.

    ```
    PULSAR_HOME/bin/pulsar-admin sinks create \
    --sink-config-file <hudi-sink-config.yaml>
    ```

@@@

:::

## Work with Function Mesh

TBD
