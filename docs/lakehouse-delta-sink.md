---
dockerfile: "https://hub.docker.com/r/streamnative/pulsar-io-lakehouse"
download: "https://github.com/streamnative/pulsar-io-lakehouse/releases"
alias: Delta Lake Sink Connector
---

The [Delta Lake](https://delta.io/) sink connector fetches data from a Pulsar topic and saves data to the Delta Lake tables.

![](/docs/lakehouse-sink.png)

## Quick start

### Prerequisites

Tips: Only support use AWS S3 as storage.

The prerequisites for connecting an Delta Lake sink connector to external systems include:

1. Create cloud buckets in **AWS S3**.
2. Create the [AWS User](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html) and create `AccessKey`(Please record `AccessKey` and `SecretAccessKey`).
3. Assign permissions to User / ServiceAccount, and ensure they have the following permissions to the AWS S3.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": [
        "{Your bucket arn}"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "{Your bucket arn}/*"
      ]
    }
  ]
}
```

### 1. Create a connector

The following command shows how to use [pulsarctl](https://github.com/streamnative/pulsarctl) to create a `builtin` connector. If you want to create a `non-builtin` connector,
you need to replace `--sink-type lakehouse-delta-lake` with `--archive /path/to/pulsar-io-lakehouse-cloud.nar`. You can find the button to download the `nar` package at the beginning of the document.

{% callout title="For StreamNative Cloud User" type="note" %}
If you are a StreamNative Cloud user, you need [set up your environment](https://docs.streamnative.io/docs/connector-setup) first.
{% /callout %}

```bash
pulsarctl sinks create \
  --sink-type lakehouse-delta-lake \
  --name lakehouse-delta-lake-sink \
  --tenant public \
  --namespace default \
  --inputs "Your topic name" \
  --parallelism 1 \
  --retainOrdering true \
  --sink-config \
  '{
    "hadoop.fs.s3a.access.key": "Your AWS access key", 
    "hadoop.fs.s3a.secret.key": "Your AWS secret access key",
    "tablePath": "Your table name, For examples: s3a://test-dev-us-west-2/lakehouse/delta_sink"
  }'
```

The `--sink-config` is the minimum necessary configuration for starting this connector, and it is a JSON string. You need to substitute the relevant parameters with your own.
If you want to configure more parameters, see [Configuration Properties](#configuration-properties) for reference.

{% callout title="Note" type="note" %}
You can also choose to use a variety of other tools to create a connector:
- [pulsar-admin](https://pulsar.apache.org/docs/3.1.x/io-use/): The command arguments for `pulsar-admin` are similar to those of `pulsarctl`. You can find an example for [StreamNative Cloud Doc](https://docs.streamnative.io/docs/connector-create#create-a-built-in-connector ).
- [RestAPI](https://pulsar.apache.org/sink-rest-api/?version=3.1.1): You can find an example for [StreamNative Cloud Doc](https://docs.streamnative.io/docs/connector-create#create-a-built-in-connector).
- [Terraform](https://github.com/hashicorp/terraform): You can find an example for [StreamNative Cloud Doc](https://docs.streamnative.io/docs/connector-create#create-a-built-in-connector).
- [Function Mesh](https://functionmesh.io/docs/connectors/run-connector): The docker image can be found at the beginning of the document.
  {% /callout %}

### 2. Send messages to the topic

{% callout title="Note" type="note" %}
If your connector is created on StreamNative Cloud, you need to authenticate your clients. See [Build applications using Pulsar clients](https://docs.streamnative.io/docs/qs-connect#jumpstart-for-beginners) for more information.
{% /callout %}

``` java
@Data
@ToString
public class TestMessage {
    private String testString;
    private int testInt;
 
    public static void main(String[] args) {
        PulsarClient client = PulsarClient.builder()
        .serviceUrl("{{Your Pulsar URL}}")
        .build();

        Producer<TestMessage> producer = client.newProducer(Schema.AVRO(TestMessage.class))
            .topic("{{Your topic name}}")
            .create();

        TestMessage testMessage = new TestMessage();
        testMessage.setTestString("test string");
        testMessage.setTestInt(123);
        for (int i = 0; i < 10; i++) {
            testMessage.setTestString("test string: " + i);
            MessageId msgID = producer.send(testMessage);
            System.out.println("Publish " + "my-message-" + i
                    + " and message ID " + msgID);
        }
        
        producer.flush();
        producer.close();
        client.close();  
    }
}
```

### 3. Use AWS Athena to query Delta Lake table
1. Check S3 bucket parquet files.
2. Login the [AWS Athena](https://aws.amazon.com/athena/)
3. Create database
```shell
create database pulsar;
```
4. Create external table delta_pulsar with the following command
```shell
CREATE EXTERNAL TABLE
  pulsar.delta_pulsar
  LOCATION '{{You tablePath, For examples: s3a://test-dev-us-west-2/lakehouse/delta_sink}}'
  TBLPROPERTIES ('table_type' = 'DELTA')
```
5. Run queries on the delta table
```shell
select * from pulsar.delta_pulsar limit 10
```
OutPut:

| test | int            |
|------|----------------|
| 123  | test string: 0 |
| 123  | test string: 1 |
| 123  | test string: 2 |
| 123  | test string: 3 |
| 123  | test string: 4 |
| 123  | test string: 5 |
| 123  | test string: 6 |
| 123  | test string: 7 |
| 123  | test string: 8 |
| 123  | test string: 9 |

## Configuration Properties

Before using the Delta lake sink connector, you need to configure it. This table outlines the properties and the
descriptions.

| Name                       | Type         | Required | Default                     | Description                                                                                                  |
|----------------------------|--------------|----------|-----------------------------|--------------------------------------------------------------------------------------------------------------|
| `hadoop.fs.s3a.access.key` | String       | true     | N/A                         | The AWS S3 access key ID. It requires permission to write objects.                                           |
| `hadoop.fs.s3a.secret.key` | String       | true     | N/A                         | The AWS S3 secret access key.                                                                                |
| `tablePath`                | String       | true     | N/A                         | The path of the Delta table.                                                                                 |
| `type`                     | String       | true     | N/A                         | The type of the Lakehouse source connector. Available values: `hudi`, `iceberg`, and `delta`.                |
| `maxCommitInterval`        | Integer      | false    | 120                         | The maximum flush interval (in units of seconds) for each batch. By default, it is set to 120s.              |
| `maxRecordsPerCommit`      | Integer      | false    | 10_000_000                  | The maximum number of records for each batch to commit. By default, it is set to `10_000_000`.               |
| `maxCommitFailedTimes`     | Integer      | false    | 5                           | The maximum commit failure times until failing the process. By default, it is set to `5`.                    |
| `sinkConnectorQueueSize`   | Integer      | false    | 10_000                      | The maximum queue size of the Lakehouse sink connector to buffer records before writing to Lakehouse tables. |
| `partitionColumns`         | List<String> | false    | Collections.emptyList()     | The partition columns for Lakehouse tables.                                                                  |
| `compression`              | String       | false    | SNAPPY                      | The compression type of the Delta Parquet file. compression type. By default, it is set to `SNAPPY`.         |
| `deltaFileType`            | String       | false    | parquet                     | The type of the Delta file. By default, it is set to `parquet`.                                              |
| `appId`                    | String       | false    | pulsar-delta-sink-connector | The Delta APP ID. By default, it is set to `pulsar-delta-sink-connector`.                                    |

> For details about this connector's advanced features and configurations, see [Advanced features](#advanced-features).

## Advanced features

### Data format types

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

