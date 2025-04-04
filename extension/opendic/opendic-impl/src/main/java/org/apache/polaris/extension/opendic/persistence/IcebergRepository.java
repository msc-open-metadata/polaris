package org.apache.polaris.extension.opendic.persistence;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Repository for managing Iceberg tables and data operations using
 * the configured REST catalog from IcebergConfig.
 */
@ApplicationScoped
public class IcebergRepository implements IBaseRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergRepository.class);
    private static final String DEFAULT_CLIENT_ID_PATH = "engineer_client_id";
    private static final String DEFAULT_CLIENT_SECRET_PATH = "engineer_client_secret";

    // Note. For now, if cache is turned off. Existence checks on records are not performed -> duplicates are allowed.
    private static final boolean IN_MEM_CACHE = true;

    // Temp non-scalable solution to O(1) duplication checks.
    // UDO/platform -> Set(existing names)
    private final IUnameCache unameCache;
    private final RESTCatalog catalog;
    private final String catalogName;

    /**
     * Creates a repository with the default catalog
     */
    public IcebergRepository() {
        this(IcebergConfig.RESTCatalogType.FILE, DEFAULT_CLIENT_ID_PATH, DEFAULT_CLIENT_SECRET_PATH);
    }

    /**
     * Creates a repository with specific client credentials
     */
    public IcebergRepository(IcebergConfig.RESTCatalogType catalogType, String clientIdPath, String clientSecretPath) {
        this.catalogName = catalogType.toString();
        var clientId = IcebergConfig.readDockerSecret(clientIdPath);
        var clientSecret = IcebergConfig.readDockerSecret(clientSecretPath);

        // Credentials not available as docker secret mounts. Pass values directly.
        if (clientId == null || clientSecret == null) {
            this.catalog = IcebergConfig.createRESTCatalog(catalogType, clientIdPath, clientSecretPath);
        } else {
            this.catalog = IcebergConfig.createRESTCatalog(catalogType, clientId, clientSecret);
        }
        LOGGER.info("Catalog {} created", catalogName);
        if (IN_MEM_CACHE) {
            Namespace systemNamespace = Namespace.of("SYSTEM");
            Namespace platformMappingsNamespace = Namespace.of("SYSTEM", "PLATFORM_MAPPINGS");

            if (!catalog.namespaceExists(systemNamespace)) {
                catalog.createNamespace(systemNamespace);
            }
            if (!catalog.namespaceExists(platformMappingsNamespace)) {
                catalog.createNamespace(platformMappingsNamespace);
            }
            unameCache = loadCache(systemNamespace, platformMappingsNamespace);
        } else {
            unameCache = new UnameCacheInMemory(new HashMap<>());
        }

    }

    /**
     * load mapNameSet. Should only be called once on startup. Loads the names of all entities into the mapNameSet for
     * uname uniqueness checking.
     */
    private IUnameCache loadCache(Namespace systemNamespace, Namespace platformMappingsNamespace) {
        LOGGER.info("Loading mapNameSet");

        var returnCache = new UnameCacheInMemory(new HashMap<>());

        if (!catalog.namespaceExists(systemNamespace)) {
            catalog.createNamespace(systemNamespace);
        }
        if (!catalog.namespaceExists(platformMappingsNamespace)) {
            catalog.createNamespace(platformMappingsNamespace);
        }
        List<TableIdentifier> udoTables = catalog.listTables(systemNamespace);
        List<TableIdentifier> platformMappingTables = catalog.listTables(platformMappingsNamespace);
        for (TableIdentifier tableId : udoTables) {
            Set<String> recordNames = readRecords(tableId).stream().map(record -> record.getField("uname").toString()).collect(Collectors.toSet());
            // SYSTEM.<UdoType> -> Set<udoName>
            returnCache.addTable(tableId);
            returnCache.addUnameEntries(tableId, recordNames);
        }
        for (TableIdentifier tableId : platformMappingTables) {
            Set<String> udoToPlatformSet = readRecords(tableId).stream().map(record -> record.getField("uname").toString()).collect(Collectors.toSet());
            // PLATFORM_MAPPINGS.<tableName> -> Set<udoType>
            returnCache.addTable(tableId);
            returnCache.addUnameEntries(tableId, udoToPlatformSet);
        }
        return returnCache;
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
    public String createTable(Namespace namespace, String tableName, Schema icebergSchema) throws AlreadyExistsException {

        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace);
        }

        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        if (catalog.tableExists(identifier)) {
            var table = catalog.loadTable(identifier);
            throw new AlreadyExistsException(
                    String.format("Type %s already exists with schema: %s", tableName, SchemaParser.toJson(table.schema())));
        }
        // Begin transaction.
        Transaction tnx = catalog.newCreateTableTransaction(identifier, icebergSchema, PartitionSpec.unpartitioned());
        if (IN_MEM_CACHE) {
            unameCache.addTable(identifier);
        }
        tnx.commitTransaction();
        return SchemaParser.toJson(tnx.table().schema());
    }

    @Override
    public void createTableIfNotExists(Namespace namespace, String tableName, Schema icebergSchema) {
        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace);
        }

        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);

        if (catalog.tableExists(identifier)) {
            catalog.loadTable(identifier);
        } else {
            Transaction tnx = catalog.newCreateTableTransaction(identifier, icebergSchema, PartitionSpec.unpartitioned());
            if (IN_MEM_CACHE) {
                unameCache.addTable(identifier);
            }
            tnx.commitTransaction();
        }
    }

    /**
     * Inserts records into an Iceberg table
     * reference: <a href="https://www.tabular.io/blog/java-api-part-3/">ref</a>
     */
    @Override
    public void insertRecords(Namespace namespace, String tableName, List<GenericRecord> records) throws IOException {
        LOGGER.info("Inserting records into table: {}.{}", namespace, tableName);

        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(identifier);

        // Begin transaction.
        Transaction tnx = table.newTransaction();
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
            LOGGER.debug("Data written to file: {}", filepath);
        }

        DataFile dataFile = dataWriter.toDataFile();
//        table.newAppend().appendFile(dataFile).commit();

        if (IN_MEM_CACHE) {
            var nameset = records.stream().map(genericRecord -> genericRecord.getField("uname").toString()).collect(Collectors.toSet());
            unameCache.addUnameEntries(identifier, nameset);
        }
        // End transaction.
        tnx.newAppend().appendFile(dataFile).commit();
        tnx.commitTransaction();

        LOGGER.debug("Data appended to table: {}", tableName);
    }

    @Override
    public void insertRecord(Namespace namespace, String tableName, GenericRecord record) throws IOException {
        var tableIdentifier = TableIdentifier.of(namespace, tableName);
        if (IN_MEM_CACHE) {
            // Precondition check
            unameCache.checkUnameDoesNotExist(tableIdentifier, record.getField("uname").toString());
        }
        insertRecords(namespace, tableName, List.of(record));
    }


    /**
     * Show tables in namespace namespaceStr
     */
    @Override
    public Map<String, String> listTablesAsStringMap(Namespace namespace) {
        return catalog.listTables(namespace)
                .stream()
                .map(TableIdentifier::name)
                .collect(Collectors.toMap(
                        tableName -> tableName,
                        tableName -> SchemaParser.toJson(catalog.loadTable(TableIdentifier.of(namespace, tableName)).schema())));
    }

    @Override
    public List<Table> listTables(Namespace namespace) {
        return catalog.listTables(namespace)
                .stream()
                .map(catalog::loadTable)
                .toList();
    }

    /**
     * Read all records fromIceberg with name={@code tableName} and namespace={@code namespace}
     * Reference: <a href="https://www.tabular.io/blog/java-api-part-3/">ref</a>
     */
    @Override
    public List<Record> readRecords(Namespace namespace, String tableName) {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(identifier);
        return readRecords(table);
    }

    @Override
    public List<Record> readRecords(TableIdentifier identifier) {
        Table table = catalog.loadTable(identifier);
        return readRecords(table);
    }

    /**
     * Read all records from an Iceberg with name={@code tableName} and namespace={@code namespace}
     * Reference: <a href="https://www.tabular.io/blog/java-api-part-3/">ref</a>
     */
    @Override
    public List<Record> readRecords(Table table) {
        List<Record> results = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            records.forEach(results::add);
            return results;
        } catch (IOException e) {
            LOGGER.error("Error reading records from table {}: {}", table.name(), e.getMessage());
            return results;
        }
    }


    /**
     * Deletes an Iceberg table
     */
    @Override
    public boolean dropTable(Namespace namespace, String tableName) {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        if (IN_MEM_CACHE) {
            var cacheDeleted = unameCache.deleteUnameTableEntry(identifier);
            assert cacheDeleted;
        }
        return catalog.dropTable(identifier);
    }


    /**
     * Deletes a single record from an Iceberg table
     *
     * @param tableName    The name of the table to delete from. Example: "function"
     * @param idColumnName The name of the ID column. Example: Name
     * @param idValue      The value in id column of the record to delete. Example: "andfunc"
     */
    @Override
    public void deleteSingleRecord(Namespace namespace, String tableName, String idColumnName, Object idValue) {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(identifier);

        Transaction tnx = table.newTransaction();

        Expression deleteExpr = Expressions.equal(idColumnName, idValue);
        tnx.newDelete().deleteFromRowFilter(deleteExpr).commit();

        if (IN_MEM_CACHE) {
            var cacheDeleted = unameCache.deleteUnameEntry(identifier, idValue.toString());
            assert cacheDeleted;
        }
        tnx.commitTransaction();

        LOGGER.debug("Deleted record with {} = {} from table: {}", idColumnName, idValue, tableName);
    }

    @Override
    public void alterAddColumn(Namespace namespace, String tableName, String columnName, String columnType) {
        throw new NotImplementedException();
    }

    @Override
    public Schema readTableSchema(Namespace namespace, String tableName) {
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