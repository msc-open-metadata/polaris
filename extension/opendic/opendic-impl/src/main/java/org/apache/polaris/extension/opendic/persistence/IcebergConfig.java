/*
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
package org.apache.polaris.extension.opendic.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergConfig.class);

  /**
   * Read a secret from Docker secret store
   *
   * @param secretName The name of the secret to read
   * @return The secret value as a string
   */
  public static String readDockerSecret(String secretName) {
    String secretPath = "/run/secrets/" + secretName;
    try {
      return Files.readString(Paths.get(secretPath)).trim(); // Remove trailing newline
    } catch (IOException e) {
      LOGGER.debug(e.getLocalizedMessage());
      return null;
    }
  }

  /**
   * Create a new preconfiguredRESTCatalog
   *
   * @return Catalog
   * @implNote <a
   *     href="https://www.tabular.io/blog/java-api-part-1/#:~:text=import,-org.apache.iceberg.catalog.Catalog;import%20org.apache.hadoop.conf.Configuration">refernce</a>
   */
  public static RESTCatalog createRESTCatalog(
      RESTCatalogType catalogType, String clientId, String clientSecret) {
    Map<String, String> conf = new HashMap<>();

    conf.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
    conf.put(
        CatalogProperties.URI,
        String.format(
            "%s/api/catalog",
            catalogType.basePath)); // Use the passed parameter instead of hardcoded value
    conf.put(CatalogProperties.WAREHOUSE_LOCATION, catalogType.catalogName);
    conf.put(CatalogProperties.FILE_IO_IMPL, catalogType.fileIoImpl);
    conf.put(OAuth2Properties.CREDENTIAL, clientId + ":" + clientSecret);
    conf.put(OAuth2Properties.SCOPE, "PRINCIPAL_ROLE:ALL");
    conf.put(
        OAuth2Properties.OAUTH2_SERVER_URI,
        String.format("%s/api/catalog/v1/oauth/tokens", catalogType.basePath));
    conf.put(OAuth2Properties.TOKEN_REFRESH_ENABLED, "true");

    RESTCatalog catalog = new RESTCatalog();
    Configuration config = new Configuration();
    catalog.setConf(config);
    catalog.initialize(catalogType.catalogName, conf);

    return catalog;
  }

  public static Catalog createRESTCatalog(String clientId, String clientSecret) {
    return createRESTCatalog(RESTCatalogType.FILE, clientId, clientSecret);
  }

  public enum RESTCatalogType {
    ADLS("AZURE_CATALOG", "org.apache.iceberg.azure.adlsv2.ADLSFileIO"),
    FILE("polaris", "org.apache.iceberg.io.ResolvingFileIO"),
    LOCAL_FILE("polaris", "org.apache.iceberg.io.ResolvingFileIO", "http://localhost:8181");

    private final String catalogName;
    private final String fileIoImpl;
    private final String basePath;

    RESTCatalogType(String catalogName, String fileIoImpl) {
      this.catalogName = catalogName;
      this.fileIoImpl = fileIoImpl;
      this.basePath = "http://polaris:8181";
    }

    RESTCatalogType(String catalogName, String fileIoImpl, String basePath) {
      this.catalogName = catalogName;
      this.fileIoImpl = fileIoImpl;
      this.basePath = basePath;
    }
  }
}
