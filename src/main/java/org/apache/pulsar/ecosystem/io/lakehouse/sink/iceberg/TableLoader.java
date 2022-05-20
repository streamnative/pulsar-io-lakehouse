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
package org.apache.pulsar.ecosystem.io.lakehouse.sink.iceberg;

import com.google.common.base.MoreObjects;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.SerializableConfiguration;

/**
 * Table loader interface.
 */
public interface TableLoader extends Closeable, Serializable {
    void open();

    Table loadTable();

    boolean exist();

    Table createTable(Schema schema, PartitionSpec spec, TableIdentifier identifier, Map<String, String> properties);

    static TableLoader fromCatalog(CatalogLoader catalogLoader, TableIdentifier identifier) {
        return new CatalogTableLoader(catalogLoader, identifier);
    }

    static TableLoader fromHadoopTable(String location) {
        return fromHadoopTable(location, PulsarCatalogFactory.getHadoopConf());
    }

    static TableLoader fromHadoopTable(String location, Configuration hadoopConf) {
        return new HadoopTableLoader(location, hadoopConf);
    }

    /**
     * Hadoop table loader.
     */
    class HadoopTableLoader implements TableLoader {
        private static final long serialVersionUID = 1L;

        private final String location;
        private final SerializableConfiguration hadoopConf;

        private transient HadoopTables tables;

        private HadoopTableLoader(String location, Configuration conf) {
            this.location = location;
            this.hadoopConf = new SerializableConfiguration(conf);
        }

        @Override
        public void open() {
            tables = new HadoopTables(hadoopConf.get());
        }

        @Override
        public boolean exist() {
            return tables.exists(location);
        }

        @Override
        public Table loadTable() {
            return tables.load(location);
        }

        @Override
        public void close() {

        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("location", location)
                .toString();
        }

        @Override
        public Table createTable(Schema schema, PartitionSpec spec,
                                 TableIdentifier identifier, Map<String, String> properties) {
            return new HadoopTables().create(schema, spec, properties, identifier.toString());
        }
    }

    /**
     * Catalog table loader.
     */
    class CatalogTableLoader implements TableLoader {

        private static final long serialVersionUID = 1L;

        private final CatalogLoader catalogLoader;
        private final String identifier;

        private transient Catalog catalog;

        private CatalogTableLoader(CatalogLoader catalogLoader, TableIdentifier tableIdentifier) {
            this.catalogLoader = catalogLoader;
            this.identifier = tableIdentifier.toString();
            this.catalog = catalogLoader.loadCatalog();
        }

        @Override
        public void open() {
            catalog = catalogLoader.loadCatalog();
        }

        @Override
        public Table loadTable() {
            return catalog.loadTable(TableIdentifier.parse(identifier));
        }

        @Override
        public boolean exist() {
            return catalog.tableExists(TableIdentifier.parse(identifier));
        }

        @Override
        public void close() throws IOException {
            if (catalog instanceof Closeable) {
                ((Closeable) catalog).close();
            }
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("tableIdentifier", identifier)
                .add("catalogLoader", catalogLoader)
                .toString();
        }

        @Override
        public Table createTable(Schema schema, PartitionSpec spec, TableIdentifier identifier,
                                 Map<String, String> properties) {
            return catalog.createTable(identifier, schema, spec, properties);
        }
    }
}
