package org.apache.polaris.extension.opendic.persistence;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


public class IcebergConfig {


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
            System.out.println("Secret " + secretName + " not found.");
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
     * @implNote Translated from our spark configuration using claude
     */
    public static Catalog createRESTCatalog(String catalogName, String clientId, String clientSecret, String catalogUri) {
        Map<String, String> conf = new HashMap<>();
        conf.put("catalog-name", catalogName);
        conf.put("warehouse", catalogName);
        conf.put("uri", catalogUri); // Use the passed parameter instead of hardcoded value
        conf.put("credential", clientId + ":" + clientSecret);
        conf.put("scope", "PRINCIPAL_ROLE:ALL");
        conf.put("token-refresh-enabled", "true");
        conf.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
        conf.put("io-impl", "org.apache.iceberg.io.ResolvingFileIO");
        RESTCatalog catalog = new RESTCatalog();
        catalog.initialize(catalogName, conf);

        System.out.println("REST Catalog configured successfully");
        return catalog;
    }
    public static Catalog createRESTCatalog(String catalogName, String clientId, String clientSecret) {
        return createRESTCatalog(catalogName, clientId, clientSecret, "http://polaris:8181/api/catalog");
    }

}