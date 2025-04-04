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

public interface IBaseRepository {
    String createTable(Namespace namespace, String tableName, Schema icebergSchema);

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

    Map<String, String> listTablesAsStringMap(Namespace namespace);

    List<Table> listTables(Namespace namespace);

    /**
     * Read records from a table
     */
    List<Record> readRecords(Namespace namespace, String tableName);

    List<Record> readRecords(TableIdentifier identifier);

    List<Record> readRecords(Table table);

    /**
     * Deletes an Iceberg {tableName}
     */
    boolean dropTable(Namespace namespace, String tableName);

    void recordWithUnameExistsCheck(TableIdentifier tableIdentifier, String uname, String idColumnName, Object idValue) throws IOException;

    /**
     * Get the schema of an table {@code namespace.tableName}
     */
    Schema readTableSchema(Namespace namespace, String tableName);

    /**
     * Deletes a single record from an Iceberg table
     *
     * @param tableName    The name of the table to delete from. Example: "function"
     * @param idColumnName The name of the ID column. Example: Name
     * @param idValue      The value in id column of the record to delete. Example: "andfunc"
     */
    void deleteSingleRecord(Namespace namespace, String tableName, String idColumnName, Object idValue);

    void alterAddColumn(Namespace namespace, String tableName, String columnName, String columnType);
    /**
     * Get the catalog instance
     */
    Catalog getCatalog();

    /**
     * Get the catalog name
     */
    String getCatalogName();
}
