package org.apache.polaris.extension.opendic.persistence;

import org.apache.hadoop.util.Lists;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Repository for managing Iceberg tables and data operations using
 * the configured REST catalog from IcebergConfig.
 */
public class IcebergRepository implements IBaseRepository {
    private final Catalog catalog;
    private final String catalogName;

    /**
     * Creates a repository with the default catalog
     */
    public IcebergRepository(String catalogName) {
        this(catalogName, "engineer_client_id", "engineer_client_secret", "http://polaris:8181/api/catalog");
    }
    public IcebergRepository(String catalogName, String baseUri) {
        this(catalogName, "engineer_client_id", "engineer_client_secret", baseUri);
    }

    /**
     * Creates a repository with specific client credentials
     */
    public IcebergRepository(String catalogName, String clientId, String clientSecret, String baseUri) {
        this.catalogName = catalogName;

        // If credentials not provided, try to read from Docker secrets
        if (clientId == null) {
            clientId = IcebergConfig.readDockerSecret(clientId);
        }

        if (clientSecret == null) {
            clientSecret = IcebergConfig.readDockerSecret(clientSecret);
        }

        // Fall back to auth token if direct credentials not available
        if (clientId == null || clientSecret == null) {
            String authToken = IcebergConfig.getAuthToken();
            if (authToken != null) {
                // Parse token to extract credentials (implementation depends on token format)
                // For this example, we'll just log that we're using token auth
                System.out.println("Using auth token for Iceberg catalog authentication");

                // In a real implementation, you might extract credentials from the token
                // or modify the createRESTCatalog method to accept a token directly
            } else {
                System.out.println("WARNING: No credentials found for Iceberg catalog");
            }
        }

        this.catalog = IcebergConfig.createRESTCatalog(catalogName, clientId, clientSecret, baseUri);
    }

    /**
     * Helper to convert map data to GenericRecord based on schema
     * Reference: <<a href="https://www.tabular.io/blog/java-api-part-3/">Reference</a>>
     */
    public static GenericRecord createGenericRecord(Schema schema, Map<String, Object> data) {
        GenericRecord record = GenericRecord.create(schema);

        return record.copy(data);
    }

    /**
     * Creates an Iceberg table from a schema definition
     */
    @Override
    public List<String> createTable(String namespace, String tableName, Schema icebergSchema) {

        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);

        // Create the table using Iceberg's API
        return catalog.createTable(
                identifier,
                icebergSchema,
                PartitionSpec.unpartitioned() // Default to unpartitioned, can be customized
        ).schema().columns().stream().map(field -> String.format("%1$s: %2$s", field.name(), field.type())).toList();

    }

    /**
     * Inserts records into an Iceberg table
     * reference: <a href="https://www.tabular.io/blog/java-api-part-3/">ref</a>
     */
    @Override
    public void insertRecords(String namespace, String tableName, List<GenericRecord> records) throws IOException {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(identifier);

        // Create a temporary file for the new data
        String filepath = table.location() + "/" + UUID.randomUUID();
        OutputFile file = table.io().newOutputFile(filepath);
        DataWriter<GenericRecord> dataWriter =
                Parquet.writeData(file)
                        .schema(table.schema())
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .overwrite()
                        .withSpec(PartitionSpec.unpartitioned())
                        .build();

        try (dataWriter) {
            for (GenericRecord record : records) {
                dataWriter.write(record);
            }
        }
        DataFile dataFile = dataWriter.toDataFile();
        table.newAppend().appendFile(dataFile).commit();
    }

    /**
     * Show tables in namespace "SYSTEM"
     *
     * @return
     */
    @Override
    public List<String> listEntityTypes(String namespaceStr) {
        Namespace namespace = Namespace.of(namespaceStr);
        return catalog.listTables(namespace).stream().map(tableIdentifier -> catalog.loadTable(tableIdentifier).name()).toList();
    }

    /**
     * Read records from an Iceberg table
     */
    @Override
    public List<GenericRecord> readRecords(String namespace, String tableName) {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(identifier);

        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            return Lists.newArrayList(records).stream()
                    .map(r -> (GenericRecord) r)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Error reading from table " + tableName, e);
        }
    }

    /**
     * Deletes an Iceberg table
     */
    @Override
    public boolean dropTable(String namespace, String tableName) {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        return catalog.dropTable(identifier);
    }


    /**
     * Get the catalog instance
     */
    @Override
    public Catalog getCatalog() {
        return catalog;
    }

    /**
     * Get the catalog name
     */
    @Override
    public String getCatalogName() {
        return catalogName;
    }
}