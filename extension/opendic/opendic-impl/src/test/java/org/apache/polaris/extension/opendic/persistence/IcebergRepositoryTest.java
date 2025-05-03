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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class IcebergRepositoryTest {
    private final Namespace TEST_NAMESPACE = Namespace.of("SYSTEM");

    @Test
    void testCreateRecord() {
        Schema functionSchema = new Schema(
                Types.NestedField.required(1, "id", Types.UUIDType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "def", Types.StringType.get()),
                Types.NestedField.required(4, "comment", Types.StringType.get()),
                Types.NestedField.required(5, "language", Types.StringType.get()),
                Types.NestedField.required(6, "runtime", Types.StringType.get()),
                Types.NestedField.required(7, "params", Types.MapType.ofRequired(1000, 1001, Types.StringType.get(), Types.StringType.get())
                ));

        // Set up test data
        Map<String, Object> data = new HashMap<>();
        data.put("id", 1L);
        data.put("name", "Foo");
        data.put("def", """
                def foo():
                    print("Hello World")
                """);
        data.put("comment", "foo function");
        data.put("language", "PYTHON");
        data.put("runtime", "3.10");
        data.put("params", Map.of("arg1", "int", "arg2", "int"));

        // Create instance of class containing the method

        // Call the method under test
        IBaseRepository icebergRepo = new IcebergRepository(IcebergCatalogConfig.RESTCatalogType.LOCAL_FILE, "root", "s3cr3t");
        GenericRecord record = icebergRepo.createGenericRecord(functionSchema, data);

        // Verify the result
        assertNotNull(record);
        assertEquals(1L, record.getField("id"));
        assertEquals("Foo", record.getField("name"));
        assertEquals("""
                def foo():
                    print("Hello World")
                """, record.getField("def"));
    }

    @Test
    void testCreateTable() {
        Schema functionSchema = new Schema(
                Types.NestedField.required(1, "id", Types.UUIDType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "def", Types.StringType.get()),
                Types.NestedField.required(4, "comment", Types.StringType.get()),
                Types.NestedField.required(5, "language", Types.StringType.get()),
                Types.NestedField.required(6, "runtime", Types.StringType.get()),
                Types.NestedField.required(7, "params", Types.MapType.ofRequired(1000, 1001, Types.StringType.get(), Types.StringType.get())
                ));

        IBaseRepository icebergRepo = new IcebergRepository(IcebergCatalogConfig.RESTCatalogType.LOCAL_FILE, "root", "s3cr3t");

        String tablename = "andreas_function";

        var columns = icebergRepo.createTable(TEST_NAMESPACE, tablename, functionSchema);
        assertNotNull(columns);
    }

    @Test
    void testDropTable() {
        IBaseRepository icebergRepo = new IcebergRepository(IcebergCatalogConfig.RESTCatalogType.LOCAL_FILE, "root", "s3cr3t");
        String tablename = "andreas_function";
        var result = icebergRepo.dropTable(TEST_NAMESPACE, tablename);
        assertTrue(result);
    }

    @Test
    void createRecordOfType() {
        // Allow security manager operations
        Schema functionSchema = new Schema(
                Types.NestedField.required(1, "id", Types.UUIDType.get()),
                Types.NestedField.required(2, "uname", Types.StringType.get()),
                Types.NestedField.required(3, "def", Types.StringType.get()),
                Types.NestedField.required(4, "comment", Types.StringType.get()),
                Types.NestedField.required(5, "language", Types.StringType.get()),
                Types.NestedField.required(6, "runtime", Types.StringType.get()),
                Types.NestedField.required(7, "params", Types.MapType.ofRequired(1000, 1001, Types.StringType.get(), Types.StringType.get())
                ));

        // Set up test data
        Map<String, Object> data = new HashMap<>();
        data.put("id", 1L);
        data.put("uname", "Foo");
        data.put("def", """
                def foo():
                    print("Hello World")
                """);
        data.put("comment", "foo function");
        data.put("language", "PYTHON");
        data.put("runtime", "3.10");
        data.put("params", Map.of("arg1", "int", "arg2", "int"));


        // Call the method under test
        IBaseRepository icebergRepo = new IcebergRepository(IcebergCatalogConfig.RESTCatalogType.LOCAL_FILE, "root", "s3cr3t");
        GenericRecord record = icebergRepo.createGenericRecord(functionSchema, data);

        String tablename = "andreas_function";

        icebergRepo.createTableIfNotExists(TEST_NAMESPACE, tablename, functionSchema);

        // Insert the record into the Iceberg table
        try {
            icebergRepo.insertRecord(TEST_NAMESPACE, tablename, record);
        } catch (IOException e) {
            fail("Failed to insert record: " + e.getMessage());
        }
        // Verify the result
        assertNotNull(record);
    }
}
