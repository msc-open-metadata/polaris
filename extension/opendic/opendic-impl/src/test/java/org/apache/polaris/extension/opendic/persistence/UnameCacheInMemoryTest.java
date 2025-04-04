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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UnameCacheInMemoryTest {

    private final Namespace TEST_UDO_NAMESPACE = Namespace.of("SYSTEM");
    private final Namespace TEST_PLATFORM_MAPPING_NAMESPACE = Namespace.of("SYSTEM", "PLATFORM_MAPPINGS");

    private UnameCacheInMemory cache;

    @BeforeEach
    void setUp() {
        // Initialize the in-memory cache
        cache = new UnameCacheInMemory();
        // Add some test data
        TableIdentifier tableName1 = TableIdentifier.of(TEST_UDO_NAMESPACE, "function");
        TableIdentifier tableName2 = TableIdentifier.of(TEST_PLATFORM_MAPPING_NAMESPACE, "snowflake");
        TableIdentifier tableName3 = TableIdentifier.of(TEST_UDO_NAMESPACE, "masking");

        cache.addTable(tableName1);
        cache.addTable(tableName2);
        cache.addTable(tableName3);

        cache.addUnameEntry(tableName1, "foo");
        cache.addUnameEntry(tableName1, "bar");
        cache.addUnameEntry(tableName1, "baz");

        cache.addUnameEntry(tableName2, "foo");
        cache.addUnameEntry(tableName2, "bar");
        cache.addUnameEntry(tableName2, "baz");

        cache.addUnameEntry(tableName3, "password_mask");
        cache.addUnameEntry(tableName3, "email_mask");
    }

    @AfterEach
    void tearDown() {
        // Clear the in-memory cache
        cache = new UnameCacheInMemory();
    }

    @Test
    void testAddUnameEntry() {
        TableIdentifier tableName = TableIdentifier.of(TEST_UDO_NAMESPACE, "function");
        String uname = "new_function";
        // Check if the uname entry was added successfully
        assertDoesNotThrow(() -> cache.addUnameEntry(tableName, uname));
        assertThrows(AlreadyExistsException.class, () -> cache.addUnameEntry(tableName, uname));
    }

    @Test
    void testDeleteTableEntry() {
        TableIdentifier tableName = TableIdentifier.of(TEST_UDO_NAMESPACE, "function");
        assertDoesNotThrow(() -> cache.deleteUnameTableEntry(tableName));
        assertThrows(NotFoundException.class, () -> cache.deleteUnameTableEntry(tableName));
    }

    @Test
    void testDeleteUnameEntry() {
        TableIdentifier tableName = TableIdentifier.of(TEST_UDO_NAMESPACE, "function");
        String uname = "foo";
        assertDoesNotThrow(() -> cache.deleteUnameEntry(tableName, uname));
        assertThrows(NotFoundException.class, () -> cache.deleteUnameEntry(tableName, uname));
    }

    @Test
    void testCheckUnameDoesNotExist() {
        TableIdentifier tableName = TableIdentifier.of(TEST_UDO_NAMESPACE, "function");
        String unameExists = "foo";
        String unameNotExists = "non_existent_function";

        assertThrows(AlreadyExistsException.class, () -> cache.checkUnameDoesNotExist(tableName, unameExists));
        assertDoesNotThrow(() -> cache.checkUnameDoesNotExist(tableName, unameNotExists));
    }
}