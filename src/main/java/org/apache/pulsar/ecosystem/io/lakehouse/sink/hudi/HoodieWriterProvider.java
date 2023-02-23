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
package org.apache.pulsar.ecosystem.io.lakehouse.sink.hudi;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.pulsar.ecosystem.io.lakehouse.common.Utils;

public class HoodieWriterProvider {

    private final HoodieSinkConfigs sinkConfigs;
    private final Configuration hadoopConf;
    private final HoodieWriteConfig defaultWriterConfig;
    private final HoodieEngineContext context;

    public HoodieWriterProvider(HoodieSinkConfigs sinkConfigs) throws IOException {
        this.sinkConfigs = sinkConfigs;
        this.hadoopConf = Utils.getDefaultHadoopConf(sinkConfigs.getProps(true));
        this.defaultWriterConfig = getDefaultConfigBuilder().build();
        this.context = new HoodieJavaEngineContext(hadoopConf);
        createTable();
    }

    private void createTable() throws IOException {
        HoodieTableMetaClient.withPropertyBuilder()
            .setTableName(defaultWriterConfig.getTableName())
            .setPayloadClassName(HoodieAvroPayload.class.getName())
            .setKeyGeneratorClassProp(defaultWriterConfig.getKeyGeneratorClass())
            .fromProperties(sinkConfigs.getProps())
            .initTable(hadoopConf, defaultWriterConfig.getBasePath());
    }

    private HoodieWriteConfig.Builder getConfigBuilderWithSchema(String schema) {
        return getDefaultConfigBuilder().withSchema(schema);
    }

    private HoodieWriteConfig.Builder getDefaultConfigBuilder() {
        return HoodieWriteConfig.newBuilder()
            .withEngineType(EngineType.JAVA)
            .withProperties(sinkConfigs.getProps())
            .withAutoCommit(false)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withInlineCompaction(false).build())
            .withClusteringConfig(HoodieClusteringConfig.newBuilder()
                .withInlineClustering(false).build());
    }

    public BufferedConnectWriter open(String schema) {
        HoodieWriteConfig config = getConfigBuilderWithSchema(schema).build();
        HoodieJavaWriteClient writeClient = new HoodieJavaWriteClient(context, config);
        return new BufferedConnectWriter(writeClient, context);
    }
}
