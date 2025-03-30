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
import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.IOUtils;
import org.apache.pulsar.ecosystem.io.lakehouse.exception.HoodieConnectorException;

@Slf4j
public class BufferedConnectWriter {

    private final HoodieJavaWriteClient writeClient;
    private final HoodieEngineContext context;
    private final HoodieWriteConfig config;
    private ExternalSpillableMap<String, HoodieRecord<?>> bufferedRecords;

    public BufferedConnectWriter(HoodieJavaWriteClient writeClient, HoodieEngineContext context) {
        this.writeClient = writeClient;
        this.context = context;
        this.config = writeClient.getConfig();
        init();
    }

    private void init() {
        try {
            // Load and batch all incoming records in a map
            long memoryForMerge = IOUtils.getMaxMemoryPerPartitionMerge(context.getTaskContextSupplier(), config);
            log.info("MaxMemoryPerPartitionMerge => " + memoryForMerge);
            this.bufferedRecords = new ExternalSpillableMap<>(memoryForMerge,
                config.getSpillableMapBasePath(),
                new DefaultSizeEstimator(),
                new HoodieRecordSizeEstimator(new Schema.Parser().parse(config.getSchema())),
                config.getCommonConfig().getSpillableDiskMapType(),
                config.getCommonConfig().isBitCaskDiskMapCompressionEnabled());
        } catch (IOException io) {
            throw new HoodieIOException("Cannot instantiate an ExternalSpillableMap", io);
        }
    }

    public void writeHoodieRecord(HoodieRecord<?> record) {
        bufferedRecords.put(record.getRecordKey(), record);
    }

    public void flushRecords(boolean upsertMode) throws HoodieConnectorException {
        final String instantTime = writeClient.startCommit();
        List<WriteStatus> writerStatusList;
        if (upsertMode) {
            writerStatusList = writeClient.upsertPreppedRecords(
                    new LinkedList<>(bufferedRecords.values()), instantTime);
        } else {
            writerStatusList = writeClient.bulkInsertPreppedRecords(
                    new LinkedList<>(bufferedRecords.values()), instantTime, Option.empty());
        }

        long totalErrorRecords = writerStatusList.stream().mapToLong(WriteStatus::getTotalErrorRecords).sum();
        long totalRecords = writerStatusList.stream().mapToLong(WriteStatus::getTotalRecords).sum();
        boolean hasErrors = totalErrorRecords > 0;
        if (!hasErrors) {
            boolean success = writeClient.commit(instantTime, writerStatusList, Option.empty());
            if (!success) {
                log.error("Commit {} failed!", instantTime);
                throw new HoodieConnectorException("Commit " + instantTime + " failed!");
            }
            log.info("Successfully flushed the records to the hudi table");
            bufferedRecords.close();
        } else {
            log.error("Found errors when committing data. Errors/Total={}/{}", totalErrorRecords, totalRecords);
            log.error("Printing out the top 50 errors");
            writerStatusList.stream().filter(WriteStatus::hasErrors)
                .limit(50)
                .forEach(ws -> {
                    log.error("Global error", ws.getGlobalError());
                    ws.getErrors().forEach((k, v) -> log.error("Error for key: {} is {}", k, v));
                });
            // TODO: do we need a rollback process?
            throw new HoodieConnectorException("Commit " + instantTime + " failed!");
        }
    }

    public HoodieWriteConfig getConfig() {
        return config;
    }

    public HoodieEngineContext getContext() {
        return context;
    }

    public void close(boolean upsertMode) throws HoodieConnectorException {
        flushRecords(upsertMode);
        writeClient.close();
    }
}
