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
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class IcebergRepositoryTest {

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
        GenericRecord record = IcebergRepository.createGenericRecord(functionSchema, data);

        // Verify the result
        assertNotNull(record);
        assertEquals(1L, record.getField("id"));
        assertEquals("Foo", record.getField("name"));
        assertEquals("""
                def foo():
                    print("Hello World")
                """, record.getField("def"));
    }
}