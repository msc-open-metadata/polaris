package org.apache.polaris.extension.opendic.persistence;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
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

import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED;
import static org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX;
import static org.apache.polaris.extension.opendic.persistence.IcebergCatalogConfig.createRESTCatalog;
import static org.apache.polaris.extension.opendic.persistence.IcebergCatalogConfig.readDockerSecret;

/**
 * Repository for managing Iceberg tables and data operations using
 * the configured REST catalog from IcebergConfig.
 */
@ApplicationScoped
public class IcebergRepository implements IBaseRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergRepository.class);

    // Note. For now, if cache is turned off. Existence checks on records are not performed -> duplicates are allowed.
    private static final boolean IN_MEM_CACHE = false;
    private static final long TARGET_FILE_SIZE_BYTES = 512 * 1024 * 1024L; // 512 MB
    private static final int SMALL_FILE_THRESHOLD = 50; // Max number of small files
    // Temp non-scalable solution to O(1) duplication checks.
    // UDO/platform -> Set(existing names)
    private final IUnameCache unameCache;
    private final RESTCatalog catalog;
    private final String catalogName;
    private CatalogProvider catalogProvider;

    /**
     * Creates a repository with the default catalog
     */
    @Inject
    public IcebergRepository(CatalogProvider catalogProvider) {
        this.catalog = catalogProvider.getCatalog();
        this.catalogName = catalogProvider.getCatalogType().toString();
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

    public IcebergRepository(IcebergCatalogConfig.RESTCatalogType catalogType, String clientIdPath, String clientSecretPath) {
        this.catalogName = catalogType.toString();
        var clientId = readDockerSecret(clientIdPath);
        var clientSecret = readDockerSecret(clientSecretPath);

        // Credentials not available as docker secret mounts. Pass values directly.
        if (clientId == null || clientSecret == null) {
            this.catalog = createRESTCatalog(catalogType, clientIdPath, clientSecretPath);
        } else {
            this.catalog = createRESTCatalog(catalogType, clientId, clientSecret);
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
        Transaction tnx = catalog.newCreateTableTransaction(identifier, icebergSchema, PartitionSpec.unpartitioned(), Map.of(
                METADATA_DELETE_AFTER_COMMIT_ENABLED, "true", METADATA_PREVIOUS_VERSIONS_MAX, "10"
        ));
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
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(identifier);

        // Create a temporary file for the new data
        String filepath = table.location() + "/" + UUID.randomUUID() + ".parquet";
        OutputFile file = table.io().newOutputFile(filepath);

        DataWriter<GenericRecord> dataWriter =
                Parquet.writeData(file)
                        .schema(table.schema())
                        .createWriterFunc(GenericParquetWriter::create)
                        .overwrite()
                        .withSpec(PartitionSpec.unpartitioned())
                        .build();

        try (dataWriter) {
            for (GenericRecord record : records) {
                recordWithUnameExistsCheck(identifier, record.getField("uname").toString(), "uname", record.getField("uname"));
                dataWriter.write(record);
            }
            LOGGER.debug("Data written to file: {}", filepath);
        }

        DataFile dataFile = dataWriter.toDataFile();

        if (IN_MEM_CACHE) {
            Set<String> nameset = records.stream().map(genericRecord -> genericRecord.getField("uname").toString()).collect(Collectors.toSet());
            unameCache.addUnameEntries(identifier, nameset);
        }
        table.newAppend().appendFile(dataFile).commit();

        if (shouldCompactFiles(table)) {
            compactFiles(table);
        }
    }

    @Override
    public void insertRecord(Namespace namespace, String tableName, GenericRecord record) throws IOException {
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
     * Simple approach to compacting tables. Production implementation would implement ActionsProvider or use spark/flink implementations
     * Note: Limitations: 1. Load data into memory. 2. Not atomic.
     * Reference: <a href="https://iceberg.apache.org/docs/nightly/java-api-quickstart/#creating-branches-and-tags">...</a>
     */
    @Override
    public void compactFiles(Table table) {
        try {
            LOGGER.info("Starting simple file compaction for table: {}", table.name());

            // Read all records from the table
            List<Record> allRecords = readRecords(table);
            if (allRecords.isEmpty()) {
                LOGGER.info("No records to compact in table: {}", table.name());
                return;
            }

            // Start a transaction
            Transaction tnx = table.newTransaction();

            // Delete all existing data
            LOGGER.debug("Deleting existing files from table: {}", table.name());
            tnx.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

            // Calculate batch size based on estimated record size and target file size
            int recordCount = allRecords.size();
            int estimatedRecordSize = 1024; // Rough estimate of record size in bytes
            int batchSize = (int) Math.min(
                    TARGET_FILE_SIZE_BYTES / estimatedRecordSize, recordCount); // Atleast recordcount

            LOGGER.debug("Rewriting {} records in batches of {} for table: {}", recordCount, batchSize, table.name());

            // Process records in batches of appropriate size
            for (int i = 0; i < recordCount; i += batchSize) {
                int end = Math.min(i + batchSize, recordCount); // We end at i+batch size or totalCount
                List<Record> batch = allRecords.subList(i, end);

                String filepath = table.location() + "/compacted-" + UUID.randomUUID() + ".parquet";
                OutputFile file = table.io().newOutputFile(filepath);

                DataWriter<Record> dataWriter = Parquet.writeData(file)
                        .schema(table.schema())
                        .createWriterFunc(GenericParquetWriter::create)
                        .overwrite()
                        .withSpec(PartitionSpec.unpartitioned())
                        .build();

                try (dataWriter) {
                    for (Record record : batch) {
                        dataWriter.write(record);
                    }
                    LOGGER.debug("Wrote batch of {} records to file: {}", batch.size(), filepath);
                }

                DataFile dataFile = dataWriter.toDataFile();
                tnx.newAppend().appendFile(dataFile).commit();
            }

            // Commit the transaction
            tnx.commitTransaction();
            LOGGER.info("Completed file compaction for table: {}. Rewrote {} records into approximately {} files.",
                    table.name(), recordCount, (int) Math.ceil((double) recordCount / batchSize));


            var retentionTime = (1000 * 60); // 1 min
            // Clean up orphaned files
            table.expireSnapshots()
                    .expireOlderThan(System.currentTimeMillis() - retentionTime) // 1 min
                    .retainLast(1)                        // Only keep the most recent snapshot
                    .cleanExpiredFiles(true)  // Remove expired data and manifest files
                    .cleanExpiredMetadata(true) // Clean expired metadata files
                    .commit();

        } catch (IOException e) {
            LOGGER.error("Failed to compact files for table {}: {}", table.name(), e.getMessage(), e);
            throw new RuntimeException("Error during file compaction: " + e.getMessage(), e);
        }
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
    public List<Record> readRecordsWithValInCol(Namespace namespace, String tableName, String col, Object val) throws
            IOException {
        TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(tableId);
        return readRecordsFiltered(table, Expressions.equal(col, val));
    }

    /**
     * Read a single record where {@code idColumName} == the id column of the table with {@code namespace.tableName} and
     * {@code idValue} == the value of that records idColumn. Example: readRecordWithId("SYSTEM", "function", "uname", "foo")
     */
    @Override
    public Record readRecordWithId(Namespace namespace, String tableName, String idColumnName, Object idValue) throws
            IOException {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(identifier);
        return readRecordWithId(table, idColumnName, idValue);
    }

    @Override
    public Record readRecordWithId(TableIdentifier tableIdentifier, String idColumnName, Object idValue) throws
            IOException {
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
    public void deleteSingleRecord(Namespace namespace, String tableName, String idColumnName, Object idValue) throws
            IOException {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(identifier);

        // Precondition check that the record exists
        if (!containsRecordWithId(identifier, idColumnName, idValue)) {
            throw new NotFoundException("Record with %s == %s does not exist in table %s", idColumnName, idValue.toString(), identifier.toString());
        }

        Expression keepFilter = Expressions.notEqual(idColumnName, idValue);
        List<Record> recordsToKeep = readRecordsFiltered(table, keepFilter);

        Transaction tnx = table.newTransaction();

        // Delete all data
        tnx.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

        // Reinsert if we have records to keep
        if (!recordsToKeep.isEmpty()) {
            String filepath = table.location() + "/" + UUID.randomUUID() + ".parquet";
            OutputFile file = table.io().newOutputFile(filepath);

            DataWriter<Record> dataWriter =
                    Parquet.writeData(file)
                            .schema(table.schema())
                            .createWriterFunc(GenericParquetWriter::create)
                            .overwrite()
                            .withSpec(PartitionSpec.unpartitioned())
                            .build();

            try (dataWriter) {
                for (Record record : recordsToKeep) {
                    dataWriter.write(record);
                }
            }
            DataFile dataFile = dataWriter.toDataFile();

            dataWriter.close();

            tnx.newAppend().appendFile(dataFile).commit();
        }

        if (IN_MEM_CACHE) {
            var cacheDeleted = unameCache.deleteUnameEntry(identifier, idValue.toString());
            assert cacheDeleted;
        }

        tnx.commitTransaction();

        LOGGER.debug("Deleted record with {} = {} from table: {}", idColumnName, idValue, tableName);
    }

    @Override
    public void replaceSingleRecord(Namespace namespace, String tableName, String idColumnName, Object
            idValue, GenericRecord record) throws IOException {
        deleteSingleRecord(namespace, tableName, idColumnName, idValue);
        insertRecord(namespace, tableName, record);
    }

    /**
     * Check if a record with the given uname exists in the table. If yes, throw exception
     * Reference: <a href="https://www.tabular.io/blog/java-api-part-2/">https://www.tabular.io/blog/java-api-part-2</a>
     *
     * @param tableIdentifier full name of table. Example: {@code "SYSTEM.function" }
     * @param uname           Example: "foo"
     * @param idColumnName    Example: uname
     * @param idValue         Example: "foo"
     */
    @Override
    public void recordWithUnameExistsCheck(TableIdentifier tableIdentifier, String uname, String
            idColumnName, Object idValue) throws IOException {
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
    public boolean containsRecordWithId(TableIdentifier tableIdentifier, String idColumnName, Object idValue) throws
            IOException {
        if (IN_MEM_CACHE) {
            return unameCache.tableContainsUname(tableIdentifier, idValue.toString());
        } else {
            Table table = catalog.loadTable(tableIdentifier);
            Expression idExpr = Expressions.equal(idColumnName, idValue);
            try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(idExpr).build()) {
                return records.iterator().hasNext();
            }
        }
    }

//    /**
//     * Note: It is regrettable that we have to import the spark runtime to use these actions.
//     * Reference: <a href="ttps://iceberg.apache.org/docs/1.8.1/maintenance/#compact-data-files">Ref</a>     *
//     *
//     * @param table the table to compact
//     */
//    @Override
//    public void compactFiles(Table table) {
//        var maxFileSizeBytes = 1024 * 1024 * 1024L; // 1024 MB
//
//        try {
//            LOGGER.info("Starting file compaction for table: {}", table.name());
//
//            // Perform data file compaction with sorting. We use SparkActions so we dont have to implement our own
//            SparkActions.get()
//                    .rewriteDataFiles(table)
//                    .options(Map.of(
//                            "rewrite-data-files", "true",
//                            "target-file-size-bytes", String.valueOf(TARGET_FILE_SIZE_BYTES),
//                            "max-file-group-size-bytes", String.valueOf(maxFileSizeBytes))
//                    )
//                    .execute();
//
//            // Compact metadata manifests
//            SparkActions
//                    .get()
//                    .rewriteManifests(table)
//                    .rewriteIf(file -> file.length() < 10 * 1024 * 1024) // 10 MB
//                    .execute();
//
//            // Remove orphaned files (files not referenced by the table metadata)
//            SparkActions.get()
//                    .deleteOrphanFiles(table)
//                    .olderThan(System.currentTimeMillis() - (1000 * 60 * 20)) // 20 min
//                    .execute();
//
//            LOGGER.info("Completed file compaction and cleanup for table: {}", table.name());
//        } catch (Exception e) {
//            LOGGER.error("Failed to compact files for table {}: {}", table.name(), e.getMessage(), e);
//        }
//    }


    @Override
    public boolean shouldCompactFiles(Table table) {
        TableScan scan = table.newScan();
        int smallFileCount = 0;
        try (CloseableIterable<FileScanTask> planfiles = scan.planFiles()) {
            for (FileScanTask fileTask : planfiles) {
                DataFile file = fileTask.file();
                if (file.fileSizeInBytes() < TARGET_FILE_SIZE_BYTES) { // 512 MB
                    smallFileCount++;
                }
            }
            LOGGER.info("Table {} has {} small files", table.name(), smallFileCount);
            return smallFileCount > SMALL_FILE_THRESHOLD;
        } catch (IOException e) {
            throw new RuntimeException("Error assessing compaction needs: " + e.getMessage(), e);
        }
    }

    // Utility method: ---------------

    /**
     * Returns the count of all reachable (non-orphaned) files in the table,
     * including both data files and metadata files that are referenced by the current table metadata.
     */
    @Override
    public Map<String, Integer> countReachableFiles(Namespace namespace, String tableName) {
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table = catalog.loadTable(identifier);
        Map<String, Integer> fileCounts = new HashMap<>();

        // Sets to track unique files. Multiple snapshots may point to the same file, so we make sure to not double count
        Set<String> dataFilePaths = new HashSet<>();
        Set<String> metadataFilePaths = new HashSet<>();

        // Count files from each snapshot
        for (Snapshot snapshot : table.snapshots()) {
            if (snapshot == null) continue;

            // Count metadata files (manifests) for this snapshot
            for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                metadataFilePaths.add(manifest.path());
            }

            // Count data files for this snapshot
            TableScan scan = table.newScan().useSnapshot(snapshot.snapshotId());
            try (CloseableIterable<FileScanTask> fileTasks = scan.planFiles()) {
                for (FileScanTask fileTask : fileTasks) {
                    dataFilePaths.add(fileTask.file().path().toString());
                }
            } catch (IOException e) {
                LOGGER.error("Error counting data files for snapshot {} of table {}: {}",
                        snapshot.snapshotId(), table.name(), e.getMessage(), e);
                throw new RuntimeException("Error counting data files: " + e.getMessage(), e);
            }
        }

        fileCounts.put("dataFiles", dataFilePaths.size());
        fileCounts.put("metadataFiles", metadataFilePaths.size());
        fileCounts.put("totalFiles", dataFilePaths.size() + metadataFilePaths.size());

        LOGGER.info("Table {} has {} data files and {} metadata files",
                table.name(), dataFilePaths.size(), metadataFilePaths.size());
        return fileCounts;
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