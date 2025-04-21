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
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
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
    private static final boolean IN_MEM_CACHE = false;

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
            try {
                unameCache = loadCache(systemNamespace, platformMappingsNamespace);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            unameCache = new UnameCacheInMemory(new HashMap<>());
        }

    }

    /**
     * load mapNameSet. Should only be called once on startup. Loads the names of all entities into the mapNameSet for
     * uname uniqueness checking.
     */
    private IUnameCache loadCache(Namespace systemNamespace, Namespace platformMappingsNamespace) throws IOException {
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
    public TableIdentifier createTable(Namespace namespace, String tableName, Schema icebergSchema) throws AlreadyExistsException {

        if (!catalog.namespaceExists(namespace)) {
            catalog.createNamespace(namespace);
        }

        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        if (catalog.tableExists(identifier)) {
            var table = catalog.loadTable(identifier);
            throw new AlreadyExistsException("Type %s already exists with schema: %s", tableName, SchemaParser.toJson(table.schema()));
        }
        // Begin transaction.
        Transaction tnx = catalog.newCreateTableTransaction(identifier, icebergSchema, PartitionSpec.unpartitioned());
        if (IN_MEM_CACHE) {
            unameCache.addTable(identifier);
        }
        tnx.commitTransaction();
        return identifier;
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

        //Precondition check
        recordWithUnameExistsCheck(tableIdentifier, record.getField("uname").toString(), "uname", record.getField("uname"));
        insertRecords(namespace, tableName, List.of(record));
    }

    /**
     * Gets the creation timestamp of a table
     *
     * @param tableIdentifier The table identifier
     * @return The creation timestamp in milliseconds since epoch
     */
    @Override
    public long getTableCreationTime(TableIdentifier tableIdentifier) {
        Table table = catalog.loadTable(tableIdentifier);
        if (table.history().isEmpty()) {
            return 0L;
        }
        HistoryEntry firstSnapshot = table.history().getFirst();
        return firstSnapshot.timestampMillis();
    }

    /**
     * Gets the last update timestamp of a table
     *
     * @param tableIdentifier The table identifier
     * @return The last update timestamp in milliseconds since epoch
     */
    @Override
    public long getTableLastUpdateTime(TableIdentifier tableIdentifier) {
        Table table = catalog.loadTable(tableIdentifier);
        if (table.history().isEmpty()) {
            return 0L;
        }
        HistoryEntry currentSnapshot = table.history().getLast();
        return currentSnapshot.timestampMillis();
    }

    /**
     * Show tables in namespace namespaceStr
     */
    @Override
    public List<TableIdentifier> listTableIds(Namespace namespace) {
        return catalog.listTables(namespace);
    }

    @Override
    public Set<String> listTableNames(Namespace namespace) {
        return catalog.listTables(namespace).stream()
                .map(tableIdentifier -> {
                    String name = tableIdentifier.name();
                    int lastDot = name.lastIndexOf('.');
                    return lastDot >= 0 ? name.substring(lastDot + 1) : name;
                })
                .collect(Collectors.toSet());
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        return catalog.listTables(namespace)
                .stream()
                .toList();
    }

    /**
     * Read all records fromIceberg with name={@code tableName} and namespace={@code namespace}
     * Reference: <a href="https://www.tabular.io/blog/java-api-part-3/">ref</a>
     */
    @Override
    public List<Record> readRecords(Namespace namespace, String tableName) throws IOException {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(identifier);
        return readRecords(table);
    }

    @Override
    public List<Record> readRecords(TableIdentifier identifier) throws IOException {
        Table table = catalog.loadTable(identifier);
        return readRecords(table);
    }

    /**
     * Read all records from an Iceberg with name={@code tableName} and namespace={@code namespace}
     * Reference: <a href="https://www.tabular.io/blog/java-api-part-3/">ref</a>
     */
    @Override
    public List<Record> readRecords(Table table) throws IOException {
        List<Record> results = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            records.forEach(results::add);
            return results;
        }
    }

    @Override
    public List<Record> readRecordsWithValInCol(Namespace namespace, String tableName, String col, Object val) throws IOException {
        TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(tableId);
        return readRecordsFiltered(table, Expressions.equal(col, val));
    }

    /**
     * Read a single record where {@code idColumName} == the id column of the table with {@code namespace.tableName} and
     * {@code idValue} == the value of that records idColumn. Example: readRecordWithId("SYSTEM", "function", "uname", "foo")
     */
    @Override
    public Record readRecordWithId(Namespace namespace, String tableName, String idColumnName, Object idValue) throws IOException {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(identifier);
        return readRecordWithId(table, idColumnName, idValue);
    }

    @Override
    public Record readRecordWithId(TableIdentifier tableIdentifier, String idColumnName, Object idValue) throws IOException {
        Table table = catalog.loadTable(tableIdentifier);
        return readRecordWithId(table, idColumnName, idValue);
    }

    @Override
    public Record readRecordWithId(Table table, String idColumnName, Object idValue) throws IOException {
        Expression idExpr = Expressions.equal(idColumnName, idValue);
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(idExpr).build()) {
            return records.iterator().next();
        }
    }

    public List<Record> readRecordsFiltered(Table table, Expression filter) throws IOException {
        List<Record> results = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(filter).build()) {
            records.forEach(results::add);
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


    /**
     * Reference: <a href="https://www.tabular.io/blog/java-api-part-2/">https://www.tabular.io/blog/java-api-part-2</a>
     *
     * @param tableIdentifier full name of table. Example: {@code "SYSTEM.function" }
     * @param uname           Example: "foo"
     * @param idColumnName    Example: uname
     * @param idValue         Example: "foo"
     */
    @Override
    public void recordWithUnameExistsCheck(TableIdentifier tableIdentifier, String uname, String idColumnName, Object idValue) throws IOException {
        if (IN_MEM_CACHE) {
            unameCache.checkUnameDoesNotExist(tableIdentifier, uname);
        } else {
            Table table = catalog.loadTable(tableIdentifier);
            Expression idExpr = Expressions.equal(idColumnName, idValue);
            try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(idExpr).build()) {
                // If !empty
                if (records.iterator().hasNext()) {
                    throw new AlreadyExistsException("Record with %s == %s already exists in table %s", idColumnName, idValue.toString(), tableIdentifier.toString());
                }
            } catch (IOException e) {
                LOGGER.error("Error checking record existence: {}", e.getMessage());
                throw new IOException("Error checking record existence", e);
            }
        }
    }

    @Override
    public boolean containsRecordWithId(TableIdentifier tableIdentifier, String idColumnName, Object idValue) throws IOException {
        if (IN_MEM_CACHE) {
            return unameCache.tableContainsUname(tableIdentifier, idColumnName);
        } else {
            Table table = catalog.loadTable(tableIdentifier);
            Expression idExpr = Expressions.equal(idColumnName, idValue);
            try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(idExpr).build()) {
                return records.iterator().hasNext();
            }
        }
    }


    @Override
    public Schema readTableSchema(Namespace namespace, String tableName) {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        return readTableSchema(identifier);
    }

    @Override
    public Schema readTableSchema(TableIdentifier tableIdentifier) {
        return catalog.loadTable(tableIdentifier).schema();
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