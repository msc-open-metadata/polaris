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
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.GenericRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IBaseRepository {
    String createTable(String namespace, String tableName, Schema icebergSchema);

    /**
     * Inserts records into an Iceberg table
     */
    void insertRecords(String namespace, String tableName, List<GenericRecord> records) throws IOException;


    /**
     * Inserts a single record into an Iceberg table
     */
    void insertRecord(String namespace, String tableName, GenericRecord record) throws IOException;

    /**
     * Creates a GenericRecord from a schema and a map of data
     */
    public GenericRecord createGenericRecord(Schema schema, Map<String, Object> data);

    Map<String, String> listEntityTypes(String namespaceStr);

    /**
     * Read records from an Iceberg table
     */
    List<GenericRecord> readRecords(String namespace, String tableName);

    /**
     * Deletes an Iceberg {tableName}
     */
    boolean dropTable(String namespace, String tableName);

    /**
     * Get the schema of an table {@code namespace.tableName}
     */
    Schema readTableSchema(String namespace, String tableName);

    /**
     * Get the catalog instance
     */
    Catalog getCatalog();

    /**
     * Get the catalog name
     */
    String getCatalogName();
}
