package org.apache.polaris.extension.opendic.persistence;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


public class IcebergConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergRepository.class);

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

    public static String getAuthToken() {
        return null;
    }

    /**
     * Create a new preconfiguredRESTCatalog
     *
     * @return Catalog
     * @implNote <a href="https://www.tabular.io/blog/java-api-part-1/#:~:text=import,-org.apache.iceberg.catalog.Catalog;import%20org.apache.hadoop.conf.Configuration">refernce</a>
     */
    public static Catalog createRESTCatalog(String catalogName, String clientId, String clientSecret, String basePath) {
        Map<String, String> conf = new HashMap<>();

        conf.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        conf.put(CatalogProperties.URI, String.format("%s/api/catalog", basePath)); // Use the passed parameter instead of hardcoded value
        conf.put(CatalogProperties.WAREHOUSE_LOCATION, catalogName);
        conf.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");
        conf.put(OAuth2Properties.CREDENTIAL, clientId + ":" + clientSecret);
        conf.put(OAuth2Properties.SCOPE, "PRINCIPAL_ROLE:ALL");
        conf.put(OAuth2Properties.OAUTH2_SERVER_URI, String.format("%s/api/catalog/v1/oauth/tokens", basePath));
        conf.put(OAuth2Properties.TOKEN_REFRESH_ENABLED, "true");

        LOGGER.debug("Creating catalog with conf:{}, {}", conf.get(CatalogProperties.URI), conf.get(OAuth2Properties.CREDENTIAL));

        RESTCatalog catalog = new RESTCatalog();
        Configuration config = new Configuration();
        catalog.setConf(config);
        catalog.initialize("polaris", conf);

        return catalog;
    }

    public static Catalog createRESTCatalog(String catalogName, String clientId, String clientSecret) {
        return createRESTCatalog(catalogName, clientId, clientSecret, "http://polaris:8181");
    }

}