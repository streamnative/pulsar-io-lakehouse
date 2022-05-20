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

import static com.google.common.base.Preconditions.checkState;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;

/**
 * Pulsar catalog factory.
 */
public class PulsarCatalogFactory {

    public static final String ICEBERG_CATALOG_TYPE = "catalog-type";
    public static final String ICEBERG_CATALOG_TYPE_HADOOP = "hadoop";
    public static final String ICEBERG_CATALOG_TYPE_HIVE = "hive";

    public static final String HIVE_CONF_DIR = "hive-conf-dir";
    public static final String DEFAULT_DATABASE = "default-database";
    public static final String DEFAULT_DATABASE_NAME = "default";
    public static final String BASE_NAMESPACE = "base-namespace";
    public static final String CACHE_ENABLED = "cache-enabled";

    public static final String TYPE = "type";
    public static final String PROPERTY_VERSION = "property-version";

    static CatalogLoader createCatalogLoader(String name, Map<String, String> properties, Configuration hadoopConf) {
        String catalogImpl = properties.get(CatalogProperties.CATALOG_IMPL);
        if (!StringUtils.isEmpty(catalogImpl)) {
            return CatalogLoader.custom(name, properties, hadoopConf, catalogImpl);
        }

        String catalogType = properties.getOrDefault(ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_TYPE_HADOOP);
        switch (catalogType.toLowerCase(Locale.ENGLISH)) {
            case ICEBERG_CATALOG_TYPE_HIVE:
                String hiveConfDir = properties.get(HIVE_CONF_DIR);
                Configuration newHadoopConf = mergeHiveConf(hadoopConf, hiveConfDir);
                return CatalogLoader.hive(name, newHadoopConf, properties);

            case ICEBERG_CATALOG_TYPE_HADOOP:
                return CatalogLoader.hadoop(name, hadoopConf, properties);

            default:
                throw new UnsupportedOperationException("Unknown catalog type: " + catalogType);
        }
    }

    private static Configuration mergeHiveConf(Configuration hadoopConf, String hiveConfDir) {
        Configuration newConf = new Configuration(hadoopConf);
        if (!StringUtils.isEmpty(hiveConfDir)) {
            checkState(Files.exists(Paths.get(hiveConfDir, "hive-site.xml")),
                "There should be a hive-site.xml file under the directory %s", hiveConfDir);
            newConf.addResource(new Path(hiveConfDir, "hive-site.xml"));
        } else {
            URL configFile = CatalogLoader.class.getClassLoader().getResource("hive-site.xml");
            if (configFile != null) {
                newConf.addResource(configFile);
            }
        }

        return newConf;
    }

    public static Configuration getHadoopConf() {
        return new Configuration();
    }

}
