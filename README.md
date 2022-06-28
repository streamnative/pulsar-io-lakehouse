## Pulsar IO :: Lakehouse Connector

The Lakehouse connector is a Pulsar IO connector for synchronizing data between Lakehouse (Delta Lake, Iceberg and Hudi) and Pulsar. It contains two types of connectors:

***Lakehouse source connector***
Currently support `DeltaLake`

This source connector can capture data changes from delta lake through [DSR](https://github.com/delta-io/connectors/wiki/Delta-Standalone-Reader) and writes data to Pulsar topics.

***Lakehouse sink connector***
Currently support `DeltaLake`, `Hudi` and `Iceberg`.

This sink connector can consume pulsar topic data and write into Lakehouse and users can use other big-data engines to process the delta lake table data further.


Currently, Lakehouse connector versions (x.y.z) are based on Pulsar versions (x.y.z).

| Delta connector version | Pulsar version                                       | Doc |
| :--------------- |:-----------------------------------------------------| :------------------------------|
2.9.x| [2.9.2](https://github.com/apache/pulsar/tree/v2.9.2)| - [Lakehouse source connector](docs/lakehouse-source.md)<br><br>- [Lakehouse sink connector](docs/lakehouse-sink.md)


Lakehouse Demos
| Lakehouse | Demo                                      |
| :--------------- |:-----------------------------------------------------|
| Delta Lake| [Delta Lake Source and Sink Demo](docs/delta-lake-demo.md)  |


## Project layout

Below are the sub folders and files of this project and their corresponding descriptions.

  ```bash
    ├── conf // stores configuration examples.
    ├── docs // stores user guides.
    ├── src // stores source codes.
    │   ├── checkstyle // stores checkstyle configuration files.
    │   ├── license // stores license headers. You can use `mvn license:format` to format the project with the stored license header.
    │   │   └── ALv2
    │   ├── main // stores all main source files.
    │   │   └── java
    │   ├── spotbugs // stores spotbugs configuration files.
    │   └── test // stores all related tests.
    │ 
  ```

## Build delta connector

Requirements:
* Java [JDK 11](https://adoptium.net/?variant=openjdk11) or [JDK 8](https://adoptium.net/?variant=openjdk8)
* Maven 3.6.1+

Compile and install without cloud dependency:

```bash
$ mvn clean install -DskipTests
```

Compile and install with cloud dependency (Including `aws`, `gcs` and `azure`):

```bash
$ mvn clean install -P cloud -DskipTests
```

Run Unit Tests:

```bash
$ mvn test
```

Run Individual Unit Test:

```bash
$ mvn test -Dtest=unit-test-name (e.g: ParquetReaderTest)
```

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
