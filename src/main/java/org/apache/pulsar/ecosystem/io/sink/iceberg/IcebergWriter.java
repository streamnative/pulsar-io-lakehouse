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

package org.apache.pulsar.ecosystem.io.sink.iceberg;

import static org.apache.pulsar.ecosystem.io.sink.iceberg.IcebergSinkConnectorConfig.HADOOP_CATALOG;
import static org.apache.pulsar.ecosystem.io.sink.iceberg.IcebergSinkConnectorConfig.HIVE_CATALOG;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.pulsar.ecosystem.io.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.exception.IncorrectParameterException;
import org.apache.pulsar.ecosystem.io.sink.LakehouseWriter;

/**
 * Iceberg writer.
 */
@Slf4j
public class IcebergWriter implements LakehouseWriter {

    private final IcebergSinkConnectorConfig config;
    private Schema schema;
    private final TableLoader tableLoader;
    private volatile TaskWriter<GenericRecord> taskWriter;
    private volatile PulsarFileCommitter fileCommitter = null;

    public IcebergWriter(SinkConnectorConfig sinkConfig, Schema schema) {
        this.config = (IcebergSinkConnectorConfig) sinkConfig;
        this.schema = schema;

        CatalogLoader catalogLoader = null;
        switch (config.catalogImpl) {
            case HADOOP_CATALOG:
                catalogLoader = CatalogLoader.hadoop(config.getCatalogName(),
                    new Configuration(), config.catalogProperties);
                break;
            case HIVE_CATALOG:
                catalogLoader = CatalogLoader.hive(config.getCatalogName(),
                    new Configuration(), config.catalogProperties);
                break;
            default:
                String errmsg = "Not support catalog: " + config.catalogImpl
                    + ", catalog name: " + config.getCatalogName();
                log.error("{}", errmsg);
                throw new IllegalArgumentException(errmsg);
        }

        TableIdentifier identifier = TableIdentifier.of(config.getTableNamespace(), config.getTableName());
        tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

        if (!tableLoader.exist()) {
            createTable(schema, tableLoader, identifier, config.getPartitionColumns(), config.getTableProperties());
        }
        tableLoader.open();

        MessageTaskWriterFactory taskWriterFactory = new MessageTaskWriterFactory(
            tableLoader.loadTable(), schema, config.parquetBatchSizeInBytes, config.fileFormat, null, false);
        // TODO specify partitionId and attemptId
        taskWriterFactory.initialize(0, 1);

        taskWriter = taskWriterFactory.create();
    }

    protected void createTable(Schema schema, TableLoader tableLoader,
                               TableIdentifier identifier, List<String> partitionsColumns,
                               Map<String, String> props) {
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);

        PartitionSpec spec = PartitionSpec.unpartitioned();
        if (partitionsColumns != null && !partitionsColumns.isEmpty()) {
            PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
            partitionsColumns.forEach(builder::identity);
            spec = builder.build();
        }
        tableLoader.createTable(icebergSchema, spec, identifier, props);
    }

    public synchronized boolean updateSchema(Schema newSchema) throws IOException {
        if (newSchema == null) {
            log.error("schema shouldn't be null");
            throw new IncorrectParameterException("schema shouldn't be null");
        }

        if (taskWriter != null && schema != null) {
            if (schema.equals(newSchema)) {
                if (log.isDebugEnabled()) {
                    log.debug("The schema is the same: {}", newSchema);
                }
                return false;
            }
            // update table schema
            // close current task write, and commit files.
            try {
                // commit files
                WriteResult writeResult = taskWriter.complete();
                getFileCommitter().commit(writeResult);

                // close task writer
                taskWriter.close();
            } catch (IOException e) {
                log.error("Failed to close iceberg writer. ", e);
                throw e;
            }

            // step2: update table schema
            checkAndUpdateIcebergTableSchema(newSchema);
            schema = newSchema;
        }

        MessageTaskWriterFactory taskWriterFactory = new MessageTaskWriterFactory(tableLoader.loadTable(),
            schema, config.parquetBatchSizeInBytes, config.fileFormat, null, false);
        taskWriterFactory.initialize(0, 1);
        taskWriter = taskWriterFactory.create();

        return true;
    }

    public void writeAvroRecord(GenericRecord record) throws IOException {
        taskWriter.write(record);
    }

    public boolean flush() {
        try {
            WriteResult writeResult = taskWriter.complete();
            getFileCommitter().commit(writeResult);
        } catch (IOException e) {
            log.error("Failed to commit. ", e);
            return false;
        }
        return true;
    }

    public synchronized PulsarFileCommitter getFileCommitter() {
        if (fileCommitter != null) {
            return fileCommitter;
        }

        fileCommitter = new PulsarFileCommitter(tableLoader, false);
        fileCommitter.initialize();
        return fileCommitter;
    }

    private void checkAndUpdateIcebergTableSchema(Schema schema) {
        Table table = tableLoader.loadTable();
        org.apache.iceberg.Schema originalIcebergSchema = table.schema();

        // pulsar schema fields
        List<Types.NestedField> pulsarSchemaFields =
            AvroSchemaUtil.convert(schema).asNestedType().asNestedType().fields();

        List<Types.NestedField> fieldsToAdd = new ArrayList<>();
        List<Types.NestedField> fieldsToRemove = new ArrayList<>();
        Set<String> pulsarSchemaFieldsNames = new HashSet<>();

        // check fields to add
        for (Types.NestedField pulsarSchemaField : pulsarSchemaFields) {
            String fieldName = pulsarSchemaField.name();
            if (originalIcebergSchema.findField(fieldName) == null) {
                fieldsToAdd.add(pulsarSchemaField);
                log.info("Fields to add: {}", pulsarSchemaField);
            }
            pulsarSchemaFieldsNames.add(fieldName);
        }

        // check fields to remove
        for (int i = 0; i < originalIcebergSchema.columns().size(); ++i) {
            String fieldName = originalIcebergSchema.columns().get(i).name();
            if (!pulsarSchemaFieldsNames.contains(fieldName)) {
                fieldsToRemove.add(originalIcebergSchema.columns().get(i));
                log.info("Fields to remove: {}", originalIcebergSchema.columns().get(i));
            }
        }

        if (fieldsToAdd.isEmpty() && fieldsToRemove.isEmpty()) {
            return;
        }

        log.info("Updating iceberg table: {} schema, fieldsToAdd size: {}, "
                + "fieldsToRemove size: {}, \n oldSchema: {}, \n new schema: {}",
            config.tableName, fieldsToAdd.size(), fieldsToRemove.size(), originalIcebergSchema, schema);

        // update schema
        UpdateSchema updateSchema = table.updateSchema();
        fieldsToAdd.forEach(field -> updateSchema.addColumn(field.name(), field.type()));
        fieldsToRemove.forEach(field -> updateSchema.deleteColumn(field.name()));
        updateSchema.commit();

        log.info("Update iceberg table: {} schema succeed. Table schema after updated: {}",
            config.tableName, table.schema().asStruct().toString());
    }

    public void close() throws IOException {
        if (taskWriter != null) {
            try {
                log.info("Start to close iceberg task writer");
                WriteResult writeResult = taskWriter.complete();
                getFileCommitter().commit(writeResult);
                taskWriter.close();
                tableLoader.close();
            } catch (IOException e) {
                log.error("Failed to close iceberg task writer. ", e);
                throw e;
            } finally {
                taskWriter = null;
            }
        }
    }
}
