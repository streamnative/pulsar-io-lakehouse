/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.sink.hudi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.pulsar.ecosystem.io.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.common.Utils;
import org.apache.pulsar.ecosystem.io.exception.HoodieConnectorException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HoodieConfigurationTest {

    @Test
    public void testHoodieWriterConfiguration() {
        SinkConnectorConfig config = new SinkConnectorConfig.DefaultSinkConnectorConfig();
        config.setProperty("type", "hudi");
        config.setProperty("hoodie.table.name", "test");
        config.setProperty("hoodie.table.type", "COPY_ON_WRITE");
        config.setProperty("hoodie.base.path", "file:///test");
        config.setProperty("hoodie.write.concurrency.mode", "optimistic_concurrency_control");
        config.setProperty("hoodie.cleaner.policy.failed.writes", "LAZY");
        HoodieSinkConfigs configs = HoodieSinkConfigs.newBuilder().withProperties(config.getProperties()).build();
        HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(configs.getProps()).build();
        System.out.println(writeConfig.getCleanerPolicy());
        System.out.println(writeConfig.getFailedWritesCleanPolicy());

    }

    @Test
    public void testLoadConfiguration() throws HoodieConnectorException {
        SinkConnectorConfig config = new SinkConnectorConfig.DefaultSinkConnectorConfig();
        config.setProperty("type", "hudi");
        config.setProperty("hoodie.table.name", "test");
        config.setProperty("hoodie.table.type", "COPY_ON_WRITE");
        config.setProperty("hoodie.base.path", "file:///test");
        config.setProperty("hadoop.fs.s3a.multipart.size", "100");
        config.setProperty("hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        String schema = "{\"type\":\"record\",\"name\":\"HoodieTestData\","
            + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";
        HoodieSinkConfigs configs = HoodieSinkConfigs.newBuilder()
            .withProperties(config.getProperties()).build();
        HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(configs.getProps()).build();
        Assert.assertEquals(writeConfig.getTableName(), "test");
        Assert.assertEquals(writeConfig.getTableType().toString(), "COPY_ON_WRITE");
        Assert.assertEquals(writeConfig.getBasePath(), "file:///test");
        Configuration hdfsConf = Utils.getDefaultHadoopConf(configs.getProps());
        Assert.assertEquals(hdfsConf.get("fs.s3a.multipart.size"), "100");
        Assert.assertEquals(hdfsConf.get("fs.s3a.aws.credentials.provider"),
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
    }
}
