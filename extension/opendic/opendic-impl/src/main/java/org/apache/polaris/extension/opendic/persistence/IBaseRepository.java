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

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IBaseRepository {
    TableIdentifier createTable(Namespace namespace, String tableName, Schema icebergSchema);

    void createTableIfNotExists(Namespace namespace, String tableName, Schema icebergSchema);

    /**
     * Inserts records into an Iceberg table
     */
    void insertRecords(Namespace namespace, String tableName, List<GenericRecord> records) throws IOException;

    /**
     * Inserts a single record into an Iceberg table
     */
    void insertRecord(Namespace namespace, String tableName, GenericRecord record) throws IOException;

    /**
     * Creates a GenericRecord from a schema and a parsed json object
     */
    GenericRecord createGenericRecord(Schema schema, Map<String, Object> data);

    List<TableIdentifier> listTableIds(Namespace namespace);

    Set<String> listTableNames(Namespace namespace);

    List<TableIdentifier> listTables(Namespace namespace);

    /**
     * Read records from a table
     */
    List<Record> readRecords(Namespace namespace, String tableName) throws IOException;

    List<Record> readRecords(TableIdentifier identifier) throws IOException;

    List<Record> readRecords(Table table) throws IOException;

    List<Record> readRecordsWithValInCol(Namespace namespace, String tableName, String col, Object val) throws IOException;

    Record readRecordWithId(Namespace namespace, String tableName, String idColumnName, Object idValue) throws IOException;

    Record readRecordWithId(TableIdentifier tableIdentifier, String idColumnName, Object idValue) throws IOException;

    Record readRecordWithId(Table table, String idColumnName, Object idValue) throws IOException;


    /**
     * Deletes an Iceberg {tableName}
     */
    boolean dropTable(Namespace namespace, String tableName);

    Map<String, Integer> countReachableFiles(Namespace namespace, String tableName);

    /**
     * Get the schema of an table {@code namespace.tableName}
     */
    Schema readTableSchema(Namespace namespace, String tableName);

    /**
     * Get the schema of an table {@code tableIdentifier}
     */
    Schema readTableSchema(TableIdentifier tableIdentifier);

    /**
     * Deletes a single record from a table
     *
     * @param tableName    The name of the table to delete from. Example: "function"
     * @param idColumnName The name of the ID column. Example: Name
     * @param idValue      The value in id column of the record to delete. Example: "func_v2"
     */
    void deleteSingleRecord(Namespace namespace, String tableName, String idColumnName, Object idValue) throws IOException;


    /**
     * Deletes and replaces an existing record.
     *
     * @param tableName    The name of the table to delete from. Example: "function"
     * @param idColumnName The name of the ID column. Should be "uname"
     * @param idValue      The value in id column of the record to delete. Example: "foo"
     * @param record       The new record that replaces the existing one. Example: { "uname": "foo", "type": "function" }
     */
    void replaceSingleRecord(Namespace namespace, String tableName, String idColumnName, Object idValue, GenericRecord record) throws IOException;

    void recordWithUnameExistsCheck(TableIdentifier tableIdentifier, String uname, String idColumnName, Object idValue) throws IOException;

    boolean containsRecordWithId(TableIdentifier tableIdentifier, String idColumnName, Object idValue) throws IOException;

    long getTableCreationTime(TableIdentifier tableIdentifier);

    long getTableLastUpdateTime(TableIdentifier tableIdentifier);

    void compactFiles(Table table);

    boolean shouldCompactFiles(Table table);

    /**
     * Get the catalog instance
     */
    Catalog getCatalog();

    /**
     * Get the catalog name
     */
    String getCatalogName();
}
