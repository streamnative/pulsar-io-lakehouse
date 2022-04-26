# Connector Development Guide

Welcome to contribute to [StreamNative Hub](https://streamnative.io/en/blog/tech/2020-05-26-intro-to-hub)! You can submit connectors around [Apache Pulsar](https://pulsar.apache.org/en/) and [StreamNatvie](https://streamnative.io/) ecosystems and host them on [StreamNative Hub](https://hub.streamnative.io/).

This instruction guides you through every step of submitting a connector to [StreamNative Hub](https://hub.streamnative.io/), including the workflow for both code and doc.

## Code

[streamnative / pulsar-io-template](https://github.com/streamnative/pulsar-io-template) is a project template for developing an enterprise-grade Pulsar connector. It sets up a project structure and contains necessary dependencies and plugins. 

To develop a connector quicker and easier, you can clone this project template.

This example develops a Pulsar connector named `pulsar-io-foo`.

1. **Create your connector project**. 

    (1) On your local machine anywhere, clone the [pulsar-io-template](https://github.com/streamnative/pulsar-io-template) to create the `pulsar-io-foo` project.

    ```bash
    git clone --bare https://github.com/streamnative/pulsar-io-template.git pulsar-io-foo
    ```

    You will get the following directories to host different files.

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

    (2) Push the `pulsar-io-foo` project to your GitHub account.

    ```
    cd pulsar-io-foo
    git push https://github.com/<your-github-account>/pulsar-io-foo
    ```

2. **Develop your connector**.

    (1) To customize your connector, update the following configurations in the [`pom`](https://github.com/streamnative/pulsar-io-template/blob/c415593b04d40a868bd3d51b5d399569a50a4b67/pom.xml) file.

    Configuration|Description
    |---|---
    `artifactId`|Update `artifactId` to your connector name.
    `version` |Update `version` to the desired connector version.<br><br>**Tip**: it is recommended to define the connector’s version NO. same as Pulsar’s version NO. In this way, it is easy to figure out the version relationships between the connector and Pulsar.
    `name`|Update `name` to `Pulsar Ecosystem :: IO Connector :: <your-connector-name>`.
    `description`|Update `description` to the descriptions of your connector.

    (2) Create a package `org.apache.pulsar.ecosystem.io.foo` in `src/main/java/org/apache/pulsar/ecosystem/io` to develop your connector logic. 

    > **Tip**
    >
    > - [Here](https://github.com/streamnative/pulsar-io-template/tree/master/src/main/java/org/apache/pulsar/ecosystem/io/random) are some code examples for developing a connector.
    >
    > - To ensure a consistent codebase, it is recommended to run the checkstyle and spotbugs. For more information, see [check your code](#check-your-code). 

3. **Test your connector**.

    Create a package `org.apache.pulsar.ecosystem.io.foo` in the `src/test` directory to develop your connector tests. For more information, see [how to write a unit test in Java](https://www.webucator.com/how-to/how-write-unit-test-java.cfm).

    > **Tip**
    > 
    > - It is strongly recommended to write tests for your connector. For more information, see [test examples](https://github.com/streamnative/pulsar-io-template/tree/master/src/test/java/org/apache/pulsar/ecosystem/io/random).
    >
    > - To ensure a consistent codebase, it is recommended to run the checkstyle and spotbugs. For more information, see [check your code](#check-your-code). 

4. **Check your code**.

    To ensure a consistent codebase, it is recommended to run the checkstyle and spotbugs, which are already set by the project template.
    
    (1) Run the checkstyle.

    ```bash
    mvn checkstyle:check
    ```

    (2) Run the spotbugs.

    ```bash
    mvn spotbugs:check
    ```

5. **Choose a license**.

    You can choose [Apache License 2.0](https://github.com/streamnative/pulsar-io-template/blob/master/LICENSE). For more information, see [choose an open source license](https://choosealicense.com/).

    After choosing the license, you need to finish the following tasks.

    (1) Replace the [`LICENSE`](https://github.com/streamnative/pulsar-io-template/blob/master/LICENSE) file in the project template with your desired license.

    (2) Add your license header to the `src/license/<your-license-header>.txt` file.

    (3) Update the license-maven-plugin configuration in the `pom.xml` file to point to your license header.

    (4) Run the `license:format` command to format the project with your license.

## Doc

Documentation is one of the maturity indicators for measuring a project. High-quality user documentation helps users to use the connector efficiently and improves users’ experiences.

You can maintain documentation using the same tools and processes that used to maintain your connector code.

1. **Create a `docs` repo**.

    In your root directory of connector repo, create a doc repo named `docs`.

    > **Note**
    >
    > Name the repo with `docs` instead of other names, or else your docs are not synced to StreamNative Hub.

    <img width="589" alt="1" src="https://user-images.githubusercontent.com/50226895/119435808-85cf0300-bd4d-11eb-97b2-ab63da049d5e.png">

2. **Write the docs** for each type of connector.

    **Example**

    - [`sqs-source.md`](https://github.com/streamnative/pulsar-io-sqs/blob/master/docs/sqs-source.md)

    - [`sqs-sink.md`](https://github.com/streamnative/pulsar-io-sqs/blob/master/docs/sqs-sink.md)
    
    <img width="705" alt="2" src="https://user-images.githubusercontent.com/50226895/119435868-a303d180-bd4d-11eb-953b-7de91608b4e0.png">

    > **Note**
    >
    > -  [Here](https://github.com/streamnative/pulsar-io-sqs/tree/master/docs) are examples
 of the SQS connector docs. It is strongly recommended to follow the doc architecture in this example, or else your docs might not be synced to StreamNative Hub.
    >
    > - The docs on these branches (master, branch-x.x.x. For example, branch-2.7.0) and tags (vx.x.x. For example, v2.7.1, v2.7.2) can be shown on the StreamNative Hub website. Or else the docs on other branches and tags are not shown on the StreamNative Hub website. Pay attention to your branch and tag names.
    > <img width="616" alt="3" src="https://user-images.githubusercontent.com/50226895/119435914-ba42bf00-bd4d-11eb-9f83-409eb78f219d.png">
    > <img width="614" alt="4" src="https://user-images.githubusercontent.com/50226895/119435917-bc0c8280-bd4d-11eb-820c-cebb74cc6c55.png">


    > **Tip**
    >
    > - To reduce maintenance costs, use a variable (for example, `{{connector:version}}`) instead of writing the specific connector version. In this way, the corresponding connector version is generated and shown automatically along with each connector release. 
    >
    > - To ensure consistency throughout all connector documentation, it is recommended to follow the [Google Developer Documentation Style Guide](https://developers.google.com/style).


3. **Sync the docs** to [StreamNative Hub](https://hub.streamnative.io/).

    Create YAML files in your connector doc repository. The backend script syncs docs from your connector repository to [StreamNative Hub](https://hub.streamnative.io/) automatically using the YAML files.

    ```
    connectors/<your-connector-repository-name>/<your-connector-repository-name>.yaml
    ```

    **Example**

    - [`sqs-source.yaml`](https://github.com/streamnative/pulsar-hub/blob/master/connectors/sqs-source/sqs-source.yaml)
    <img width="744" alt="6" src="https://user-images.githubusercontent.com/50226895/119436182-48b74080-bd4e-11eb-9db6-e0e7ea68de50.png">
    
    - [`sqs-sink.yaml`](https://github.com/streamnative/pulsar-hub/blob/master/connectors/sqs-sink/sqs-sink.yaml)
    <img width="747" alt="7" src="https://user-images.githubusercontent.com/50226895/119436191-49e86d80-bd4e-11eb-942a-cd078dc745d7.png">


4. Send a PR and **request docs review**.

    Your doc will be reviewed by connector maintainers. You will work together to finalize the doc.

    [Here](https://github.com/streamnative/pulsar-hub/pull/139/files) is a PR example.
    
## Blog

After finishing the code and documentation, you can create a post to announce the connector. You can contact us to publish it on the [StreamNative website](https://streamnative.io/blog) and we can work together to promote it to multiple channels.

**Example**

- [Announcing AWS SQS Connector for Apache Pulsar](https://streamnative.io/en/blog/tech/2021-03-17-announcing-aws-sqs-connector-for-apache-pulsar)

## More info

If you have any questions about contributing your connector to [StreamNative Hub](https://hub.streamnative.io/), feel free to [open an issue](https://github.com/streamnative/pulsar-io-template/issues/new/choose) to discuss it with us or [contact us](https://streamnative.io/en/contact). We look forward to your contribution!
