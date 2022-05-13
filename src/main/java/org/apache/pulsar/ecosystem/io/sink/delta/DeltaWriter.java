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

package org.apache.pulsar.ecosystem.io.sink.delta;

import io.delta.standalone.CommitResult;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Format;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.SetTransaction;
import io.delta.standalone.types.StructType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.pulsar.ecosystem.io.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.common.SchemaConverter;
import org.apache.pulsar.ecosystem.io.parquet.DeltaParquetFileWriter;
import org.apache.pulsar.ecosystem.io.parquet.DeltaParquetWriter;
import org.apache.pulsar.ecosystem.io.parquet.PartitionedDeltaParquetFileWriter;
import org.apache.pulsar.ecosystem.io.sink.LakehouseWriter;

/**
 * Delta writer used for write record into delta lake parquet file and then commit the snapshot.
 */
@Slf4j
@Data
public class DeltaWriter implements LakehouseWriter {
    protected static final String NAME = "metadata";
    protected static final String DESCRIPTION = "metadata change";
    protected static final String COMMIT_INFO = "pulsar-sink-connector-version-2.9.1";

    private final DeltaSinkConnectorConfig config;
    private final String appId;
    private DeltaLog deltaLog;
    private final Schema schema;
    private DeltaParquetWriter writer;
    private Random random;

    public DeltaWriter(SinkConnectorConfig cfg, Schema schema) {
        this.config = (DeltaSinkConnectorConfig) cfg;
        this.appId = this.config.getAppId();
        this.schema = schema;

        Configuration configuration = new Configuration();
        deltaLog = DeltaLog.forTable(configuration, config.tablePath);
        random = new Random(42);

        if (!deltaLog.tableExists()) {
            createTable(schema, config.getPartitionColumns());
        }

        if (config.getPartitionColumns() != null && !config.getPartitionColumns().isEmpty()) {
            writer = new PartitionedDeltaParquetFileWriter(configuration,
                config.tablePath, config.getPartitionColumns(), config.compression, schema);
        } else {
            writer = new DeltaParquetFileWriter(configuration, config.tablePath, config.compression, schema);
        }
    }

    @Override
    public void close() {
        try {
            commitFiles(writer.closeAndFlush());
        } catch (IOException e) {
            log.error("Failed to close and commit parquet file into delta lake. ", e);
        }
    }

    @Override
    public void writeAvroRecord(GenericRecord record) throws IOException {
        writer.writeToParquetFile(record);
    }

    @Override
    public boolean flush() {
        try {
            commitFiles(writer.closeAndFlush());
            return true;
        } catch (IOException e) {
            log.error("Failed to close and commit parquet file into delta lake. ", e);
            return false;
        }
    }

    protected void createTable(Schema schema, List<String> partitionsColumns) {
        OptimisticTransaction optimisticTransaction = deltaLog.startTransaction();
        String uuid = UUID.randomUUID().toString();
        StructType structType = SchemaConverter.convertAvroSchemaToDeltaSchema(schema);
        Metadata metadata = new Metadata(uuid, NAME, "create table",
            new Format(config.getDeltaFileType(), Collections.emptyMap()),
            partitionsColumns,
            Collections.emptyMap(),
            Optional.of(System.currentTimeMillis()), structType);
        SetTransaction setTransaction = new SetTransaction(appId, optimisticTransaction.txnVersion(appId) + 1,
            Optional.of(System.currentTimeMillis()));
        List<Action> filesToCommit = new ArrayList<>();
        filesToCommit.add(setTransaction);
        filesToCommit.add(metadata);

        optimisticTransaction.commit(filesToCommit, new Operation(Operation.Name.CREATE_TABLE), COMMIT_INFO);
        log.info("create table delta schema succeed: {}", metadata.getSchema());
    }

    public boolean updateSchema(Schema schema) throws IOException {
        if (this.schema.equals(schema)) {
            return false;
        }

        // close and flush current writer
        commitFiles(writer.closeAndFlush());

        // update delta table schema
        int cnt = 0;
        while (true) {
            try {
                cnt++;
                OptimisticTransaction optimisticTransaction = deltaLog.startTransaction();
                StructType structType = SchemaConverter.convertAvroSchemaToDeltaSchema(schema);
                Metadata metadata = optimisticTransaction.metadata().copyBuilder().schema(structType).build();
                optimisticTransaction.updateMetadata(metadata);
                List<Action> filesToCommit = new ArrayList<>();
                optimisticTransaction.commit(filesToCommit, new Operation(Operation.Name.UPGRADE_SCHEMA), COMMIT_INFO);
                log.info("update delta schema succeed. {}",
                    metadata.getSchema() != null ? metadata.getSchema().getTreeString() : null);
                break;
            } catch (Exception e) {
                if (cnt >= 5) {
                    log.error("Failed to update delta schema. ", e);
                    throw e;
                }

                try {
                    Thread.sleep(random.nextInt(1000));
                } catch (InterruptedException ex) {
                    //
                }
            }
        }

        writer.updateSchema(schema);
        return true;
    }

    protected void commitFiles(List<DeltaParquetWriter.FileStat> fileStats) {
        if (fileStats == null || fileStats.isEmpty()) {
            return;
        }

        OptimisticTransaction optimisticTransaction = deltaLog.startTransaction();
        SetTransaction setTransaction = new SetTransaction(appId, optimisticTransaction.txnVersion(appId) + 1,
            Optional.of(System.currentTimeMillis()));
        List<Action> filesToCommit = new ArrayList<>();
        filesToCommit.add(setTransaction);

        for (DeltaParquetWriter.FileStat fileStat : fileStats) {
            log.info("add filePath: {}, partitionValues: {}, fileSize: {}",
                fileStat.getFilePath(), fileStat.getPartitionValues(), fileStat.getFileSize());
            AddFile addFile = new AddFile(fileStat.getFilePath(), fileStat.getPartitionValues(), fileStat.getFileSize(),
                System.currentTimeMillis(), true, "\"{}\"", null);
            filesToCommit.add(addFile);
        }

        log.info("Add parquet files: {}", filesToCommit);
        CommitResult commitResult =
            optimisticTransaction.commit(filesToCommit, new Operation(Operation.Name.WRITE), COMMIT_INFO);

        log.info("Commit to delta table succeed for fileStat size: {}, commit version: {}",
            fileStats.size(), commitResult.getVersion());
    }
}
