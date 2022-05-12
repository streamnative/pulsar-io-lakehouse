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

import java.util.Locale;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.pulsar.ecosystem.io.SinkConnectorConfig;
import org.apache.pulsar.ecosystem.io.common.FieldContext;

/**
 * Iceberg specific config.
 */
@Slf4j
@Data
public class IcebergSinkConnectorConfig extends SinkConnectorConfig {

    protected static final String HIVE_CATALOG = "hiveCatalog";
    protected static final String HADOOP_CATALOG = "hadoopCatalog";
    protected static final String DEFAULT_CATALOG_NAME = "icebergSinkConnector";

    @FieldContext(
        category = CATEGORY_SINK,
        doc = "Iceberg table properties"
    )
    Map<String, String> tableProperties;

    @FieldContext(
        category = CATEGORY_SINK,
        required = true,
        doc = "Iceberg catalog properties"
    )
    Map<String, String> catalogProperties;

    @FieldContext(
        category = CATEGORY_SINK,
        doc = "Iceberg catalog name"
    )
    String catalogName = DEFAULT_CATALOG_NAME;

    @FieldContext(
        category = CATEGORY_SINK,
        required = true,
        doc = "Iceberg table namespace"
    )
    String tableNamespace;

    @FieldContext(
        category = CATEGORY_SINK,
        required = true,
        doc = "Iceberg table name"
    )
    String tableName;

    String catalogImpl;
    int parquetBatchSizeInBytes;
    String warehouse;
    FileFormat fileFormat;

    @Override
    public void validate() throws IllegalArgumentException {
        super.validate();

        if (catalogProperties == null
            || StringUtils.isBlank(tableNamespace)
            || StringUtils.isBlank(tableName)) {
            String msg = "catalogProperties, tableNamespace and tableName are required.";
            log.error("{}", msg);
            throw new IllegalArgumentException(msg);
        }

        catalogImpl = String.valueOf(catalogProperties.getOrDefault(CatalogProperties.CATALOG_IMPL,
            HADOOP_CATALOG));
        warehouse = String.valueOf(catalogProperties.get(CatalogProperties.WAREHOUSE_LOCATION));

        String formatString = "";
        if (tableProperties != null) {
            parquetBatchSizeInBytes = Integer.parseInt(tableProperties.getOrDefault(
                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT));
            formatString = tableProperties.getOrDefault(TableProperties.DEFAULT_FILE_FORMAT,
                TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        } else {
            parquetBatchSizeInBytes = Integer.parseInt(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT);
            formatString = TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
        }
        fileFormat = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));

        if (StringUtils.isBlank(warehouse)) {
            String msg = CatalogProperties.WAREHOUSE_LOCATION + " must be set.";
            log.error("{}", msg);
            throw new IllegalArgumentException(msg);
        }

        if (!catalogImpl.equals(HADOOP_CATALOG) && !catalogImpl.equals(HIVE_CATALOG)) {
            String msg = "catalogImpl should be hadoopCatalog or hiveCatalog";
            log.error("{}", msg);
            throw new IllegalArgumentException(msg);
        }
    }

}
