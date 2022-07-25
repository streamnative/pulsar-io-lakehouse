# Hudi sink connector example

# Run the Hudi sink with standalone Pulsar service

## Prerequirment

- [Pulsar 2.10.1]([https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-2.10.1/apache-pulsar-2.10.1-bin.tar.gz](https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-2.10.1/apache-pulsar-2.10.1-bin.tar.gz))
- [lakehouse connector 2.10.1.1]([https://github.com/streamnative/pulsar-io-lakehouse/releases/tag/v2.10.1.1](https://github.com/streamnative/pulsar-io-lakehouse/releases/tag/v2.10.1.1)) (both of the connectors with/without the `cloud` suffix need to download)
- Python3
- pyspark 3.2.1

 

Initialize the pyspark environment.

1. Create a virtualenv with python3.
    
    ```bash
    python3 -m venv .hudi-pyspark && source .hudi-pyspark/bin/activate
    ```
    
2. Download pyspark.
    
    ```bash
    pip install pyspark==3.2.1 && export PYSPARK_PYTHON=$(which python3)
    ```
    

## Store table in Local Filesystem

1. Decompress the pulsar package and enter it.
    
    ```json
    tar -xvf apache-pulsar-2.10.1-bin.tar.gz && cd apache-pulsar-2.10.1
    ```
    
2. Start the pulsar service with daemon service.
    
    ```bash
    bin/pulsar-daemon start standalone
    ```
    
3. Make a directory for storing table data.
    
    ```json
    mkdir hudi-sink
    ```
    
4. Create the sink configuration file `hudi-sink.json`. Remember to update the `archive` and `hoodie.base.path` to the correct path.
    
    ```json
    {
         "tenant": "public",
         "namespace": "default",
         "name": "hudi-sink",
         "inputs": [
           "test-hudi-pulsar"
         ],
         "archive": "/path/to/pulsar-io-lakehouse-2.10.1.1.nar",
         "parallelism": 1,
         "processingGuarantees": "EFFECTIVELY_ONCE",
         "configs":   {
             "type": "hudi",
             "hoodie.table.name": "hudi-connector-test",
             "hoodie.table.type": "COPY_ON_WRITE",
             "hoodie.base.path": "file:///path/to/hudi-sink",
             "hoodie.datasource.write.recordkey.field": "id",
             "hoodie.datasource.write.partitionpath.field": "id",
    				 "maxRecordsPerCommit": "10"
         }
     }
    ```
    
5. Submit the hudi sink with pulsar-admin.
    
    ```bash
    bin/pulsar-admin sinks create --sink-config-file ${PWD}/hudi-sink.json
    ```
    
6. Check the sink status to confirm it is running.
    
    ```bash
    bin/pulsar-admin sinks status --name hudi-sink
    ```
    
    Then it will output like this:
    
    ```bash
    {
      "numInstances" : 1,
      "numRunning" : 1,
      "instances" : [ {
        "instanceId" : 0,
        "status" : {
          "running" : true,
          "error" : "",
          "numRestarts" : 0,
          "numReadFromPulsar" : 0,
          "numSystemExceptions" : 0,
          "latestSystemExceptions" : [ ],
          "numSinkExceptions" : 0,
          "latestSinkExceptions" : [ ],
          "numWrittenToSink" : 0,
          "lastReceivedTime" : 0,
          "workerId" : "c-standalone-fw-localhost-8080"
        }
      } ]
    }
    ```
    
    The `numRunning` shows `1` and the `running` shows `true` that means the sink start successfully.
    
7. Produce 100 messages to the topic `test-hudi-pulsar` to make the hudi flush records to the table `hudi-connector-test`.
    
    ```bash
    for i in {1..10}; do bin/pulsar-client produce -vs 'json:{"type":"record","name":"data","fields":[{"name":"id","type":"int"}]}' -m "{\"id\":$i}" test-hudi-pulsar; done
    ```
    
8. Check the sink status to confirm the message consumed
    
    ```jsx
    bin/pulsar-admin sinks status --name hudi-sink
    ```
    
    Then it will output like this:
    
    ```jsx
    {
      "numInstances" : 1,
      "numRunning" : 1,
      "instances" : [ {
        "instanceId" : 0,
        "status" : {
          "running" : true,
          "error" : "",
          "numRestarts" : 0,
          "numReadFromPulsar" : 10,
          "numSystemExceptions" : 0,
          "latestSystemExceptions" : [ ],
          "numSinkExceptions" : 0,
          "latestSinkExceptions" : [ ],
          "numWrittenToSink" : 10,
          "lastReceivedTime" : 1657637475669,
          "workerId" : "c-standalone-fw-localhost-8080"
        }
      } ]
    }
    ```
    
    Then `numReadFromPulsar` shows `10` and `numWrittenToSink` shows `10` that means the messages are written into the sink.
    
9. Start pyspark with hudi.
    
    ```bash
    pyspark \
    --packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.11.1 \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
    --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
    ```
    
10. Execute the following code in the pyspark.
    
    ```python
    tablename="hudi-connector-test"
    basepath="file:///path/to/hudi-sink"
    tripsSnapshotDF = spark.read.format("hudi").load(basepath)
    tripsSnapshotDF.createOrReplaceTempView("pulsar")
    spark.sql("select id from pulsar").show()
    ```
    
    Then it will show the table `hudi-connector-test` content which is produced from the pulsar's topic `test-hudi-pulsar`.
    
    ```python
    +---+
    | id|
    +---+
    | 10|
    |  9|
    |  1|
    |  7|
    |  6|
    |  5|
    |  3|
    |  8|
    |  4|
    |  2|
    +---+
    ```
    

## Store table in Cloud Object Storage

- Note
    
    Because this tutorial will use cloud object storage, you need to configure all the related stuff about the cloud object storage before the following steps. The following example needs a bucket and makes sure you have the correct permissions to access it. 
    

This example shows using an S3 bucket `hudi-sink` as the hudi table storage.

1. Decompress the pulsar package and enter it.
    
    ```json
    tar -xvf apache-pulsar-2.10.1-bin.tar.gz && cd apache-pulsar-2.10.1
    ```
    
2. Start the pulsar service with daemon service.
    
    ```bash
    bin/pulsar-daemon start standalone
    ```
    
3. Make a directory for storing table data.
    
    ```json
    mkdir hudi-sink
    ```
    
4. Create the sink configuration file `hudi-sink.json`. Remember to update the `archive` and `hoodie.base.path` to the correct path. And the `archive` is using the connector with `cloud` suffix.
    
    ```json
    {
         "tenant": "public",
         "namespace": "default",
         "name": "hudi-sink",
         "inputs": [
           "test-hudi-pulsar"
         ],
         "archive": "/path/to/pulsar-io-lakehouse-2.10.1.1-cloud.nar",
         "parallelism": 1,
         "processingGuarantees": "EFFECTIVELY_ONCE",
         "configs":   {
             "type": "hudi",
             "hoodie.table.name": "hudi-connector-test",
             "hoodie.table.type": "COPY_ON_WRITE",
             "hoodie.base.path": "s3a://hudi-sink/hudi-connector-test",
             "hoodie.datasource.write.recordkey.field": "id",
             "hoodie.datasource.write.partitionpath.field": "id",
    				 "maxRecordsPerCommit": "10",
             "hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
         }
     }
    ```
    
5. Submit the hudi sink with pulsar-admin.
    
    ```bash
    bin/pulsar-admin sinks create --sink-config-file ${PWD}/hudi-sink.json
    ```
    
6. Check the sink status to confirm it is running.
    
    ```bash
    bin/pulsar-admin sinks status --name hudi-sink
    ```
    
    Then it will output like this:
    
    ```bash
    {
      "numInstances" : 1,
      "numRunning" : 1,
      "instances" : [ {
        "instanceId" : 0,
        "status" : {
          "running" : true,
          "error" : "",
          "numRestarts" : 0,
          "numReadFromPulsar" : 0,
          "numSystemExceptions" : 0,
          "latestSystemExceptions" : [ ],
          "numSinkExceptions" : 0,
          "latestSinkExceptions" : [ ],
          "numWrittenToSink" : 0,
          "lastReceivedTime" : 0,
          "workerId" : "c-standalone-fw-localhost-8080"
        }
      } ]
    }
    ```
    
    The `numRunning` shows `1` and the `running` shows `true` that means the sink start successfully.
    
7. Produce 100 messages to the topic `test-hudi-pulsar` to make the hudi flush records to the table `hudi-connector-test`.
    
    ```bash
    for i in {1..10}; do bin/pulsar-client produce -vs 'json:{"type":"record","name":"data","fields":[{"name":"id","type":"int"}]}' -m "{\"id\":$i}" test-hudi-pulsar; done
    ```
    
8. Check the sink status to confirm the message consumed
    
    ```jsx
    bin/pulsar-admin sinks status --name hudi-sink
    ```
    
    Then it will output like this:
    
    ```jsx
    {
      "numInstances" : 1,
      "numRunning" : 1,
      "instances" : [ {
        "instanceId" : 0,
        "status" : {
          "running" : true,
          "error" : "",
          "numRestarts" : 0,
          "numReadFromPulsar" : 10,
          "numSystemExceptions" : 0,
          "latestSystemExceptions" : [ ],
          "numSinkExceptions" : 0,
          "latestSinkExceptions" : [ ],
          "numWrittenToSink" : 10,
          "lastReceivedTime" : 1657637475669,
          "workerId" : "c-standalone-fw-localhost-8080"
        }
      } ]
    }
    ```
    
    Then `numReadFromPulsar` shows `10` and `numWrittenToSink` shows `10` that means the messages are written into the sink.
    
9. Start pyspark with hudi.
    
    ```bash
    pyspark \
    --packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.11.1,org.apache.hadoop:hadoop-aws:3.3.1 \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
    --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
    ```
    
10. Execute the following code in the pyspark.
    
    ```python
    tablename="hudi-connector-test"
    basepath="s3a://hudi-sink/hudi-connector-test"
    tripsSnapshotDF = spark.read.format("hudi").load(basepath)
    tripsSnapshotDF.createOrReplaceTempView("pulsar")
    spark.sql("select id from pulsar").show()
    ```
    
    Then it will show the table `hudi-connector-test` content which is produced from the pulsar's topic `test-hudi-pulsar`.