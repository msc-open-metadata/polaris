package org.apache.polaris.extension.opendic.persistence;

import org.apache.hadoop.util.Lists;
import org.apache.iceberg.*;
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
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergRepository.class);
    private final RESTCatalog catalog;
    private final String catalogName;

    /**
     * Creates a repository with the default catalog
     */
    public IcebergRepository(String catalogName) {
        this(catalogName, "engineer_client_id", "engineer_client_secret", "http://polaris:8181");
    }

    public IcebergRepository(String catalogName, String baseUri) {
        this(catalogName, "engineer_client_id", "engineer_client_secret", baseUri);
    }

    /**
     * Creates a repository with specific client credentials
     */
    public IcebergRepository(String catalogName, String clientIdPath, String clientSecretPath, String baseUri) {
        this.catalogName = catalogName;
        var clientId = IcebergConfig.readDockerSecret(clientIdPath);
        var clientSecret = IcebergConfig.readDockerSecret(clientSecretPath);

        // Credentials not available as docker secret mounts. Pass values directly.
        if (clientId == null || clientSecret == null) {
            this.catalog = IcebergConfig.createRESTCatalog(catalogName, clientIdPath, clientSecretPath, baseUri);
        } else {
            this.catalog = IcebergConfig.createRESTCatalog(catalogName, clientId, clientSecret, baseUri);
        }
        LOGGER.info("Catalog {} created", catalogName);
    }

    /**
     * Helper to convert map data to GenericRecord based on schema
     * Reference: <<a href="https://www.tabular.io/blog/java-api-part-3/">Reference</a>>
     */
    @Override
    public GenericRecord createGenericRecord(Schema schema, Map<String, Object> data) {
        GenericRecord record = GenericRecord.create(schema);
        return record.copy(data);
    }

    /**
     * Creates an Iceberg table from a schema definition
     *
     * @return JSON or a string representation of the tableschema.
     * @implNote <a href="https://www.tabular.io/blog/java-api-part-3/">ref</a>
     */
    @Override
    public String createTable(String namespace, String tableName, Schema icebergSchema) throws AlreadyExistsException {
        Namespace namespaceObj = Namespace.of(namespace);

        if (!catalog.namespaceExists(namespaceObj)) {
            catalog.createNamespace(namespaceObj);
        }

        TableIdentifier identifier = TableIdentifier.of(namespaceObj, tableName);
        if (catalog.tableExists(identifier)) {
            var table = catalog.loadTable(identifier);
            return SchemaParser.toJson(table.schema());
        }
        Table table = catalog.createTable(identifier, icebergSchema, PartitionSpec.unpartitioned());
        // Create the table using Iceberg's API
        return SchemaParser.toJson(table.schema());
    }

    /**
     * Inserts records into an Iceberg table
     * reference: <a href="https://www.tabular.io/blog/java-api-part-3/">ref</a>
     */
    @Override
    public void insertRecords(String namespace, String tableName, List<GenericRecord> records) throws IOException {
        Namespace namespaceObj = Namespace.of(namespace);
        TableIdentifier identifier = TableIdentifier.of(namespaceObj, tableName);
        Table table = catalog.loadTable(identifier);

        // Create a temporary file for the new data
        String filepath = table.location() + "/" + UUID.randomUUID();
        OutputFile file = table.io().newOutputFile(filepath);
        DataWriter<GenericRecord> dataWriter = Parquet.writeData(file)
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

    @Override
    public void insertRecord(String namespace, String tableName, GenericRecord record) throws IOException {
        insertRecords(namespace, tableName, List.of(record));
    }


    /**
     * Show tables in namespace namespaceStr
     */
    @Override
    public List<String> listEntityTypes(String namespaceStr) {
        Namespace namespace = Namespace.of(namespaceStr);
        return catalog.listTables(namespace)
                .stream()
                .map(tableIdentifier -> catalog.loadTable(tableIdentifier).schema())
                .map(SchemaParser::toJson)
                .toList();
    }

    /**
     * Read all records from an Iceberg with name={@code tableName} and namespace={@code namespace}
     */
    @Override
    public List<GenericRecord> readRecords(String namespace, String tableName) {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(identifier);

        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            return Lists.newArrayList(records).stream().map(r -> (GenericRecord) r).collect(Collectors.toList());
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

    @Override
    public Schema readTableSchema(String namespace, String tableName) {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        return catalog.loadTable(identifier).schema();
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