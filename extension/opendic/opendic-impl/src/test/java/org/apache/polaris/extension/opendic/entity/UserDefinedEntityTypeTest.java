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

package org.apache.polaris.extension.opendic.entity;

import org.apache.avro.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class UserDefinedEntityTypeTest {

    static Stream<Arguments> propertyTypeProvider() {
        return Stream.of(
                Arguments.of(null, 0, "Null map should return empty result"),
                Arguments.of(new HashMap<>(), 0, "Empty map should return empty resul"),
                Arguments.of(Map.of("def", "string", "language", "string"), 2, "Map with two properties should return two definitions"),
                Arguments.of(
                        Map.of("num", "number", "flag", "boolean", "obj", "list"),
                        3,
                        "Map with different property types should convert correctly"
                )
        );
    }

    @ParameterizedTest
    @MethodSource("propertyTypeProvider")
    void test_001_fromMap(Map<String, String> propMap, int expectedSize, String msg) {
        // Test with a null map
        Map<String, UserDefinedEntityType.PropertyType> result = UserDefinedEntityType.propsFromMap(propMap);
        assertNotNull(result);
        assertEquals(expectedSize, result.size());

        assertEquals(expectedSize, result.size(), msg);

        // Additional verification for non-null cases
        if (propMap != null) {
            for (String key : propMap.keySet()) {
                assertTrue(result.containsKey(key), "Result should contain key: " + key);
            }
        }
    }


    static Stream<Arguments> schemaPropertyTypeProvider() {
        return Stream.of(
                Arguments.of("Function", Map.of("def", "variant", "language", "string")),
                Arguments.of("User", Map.of("username", "string", "password", "string")),
                Arguments.of("MaskingPolicy", Map.of("policy", "variant", "password", "string"))
        );}

    @ParameterizedTest
    @MethodSource("schemaPropertyTypeProvider")
    void test_002_generateSchema(String typeName, Map<String, String> props) {

        UserDefinedEntityType entityType = new UserDefinedEntityType.Builder(typeName)
                .setProperties(UserDefinedEntityType.propsFromMap(props))
                .build();

        Schema schema = UserDefinedEntityType.generateSchema(entityType);

        assertNotNull(schema, "Schema should not be null");
        assertEquals(Schema.Type.RECORD, schema.getType(), "Schema type should be RECORD");
        assertEquals(typeName, schema.getName(), "Schema name should match the entity type name");
        assertTrue(schema.hasFields());
        List<String> fieldStream = schema.getFields().stream().map(Schema.Field::name).takeWhile(typeString -> props.containsKey(typeString)).toList();
        assertEquals(fieldStream.size(), props.size());
    }
}