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

import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.polaris.extension.opendic.model.CreatePlatformMappingRequest;
import org.apache.polaris.extension.opendic.model.PlatformMapping;
import org.apache.polaris.extension.opendic.model.PlatformMappingObjectDumpMapValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class UserDefinedPlatformMappingTest {

    @Test
    void fromRequest() {
        var expectedSyntax =
                "CREATE OR ALTER <type> <signature>\n    RETURNS <return_type>\n    LANGUAGE <language>\n    RUNTIME = <runtime>\n    HANDLER = '<name>'\n    AS $$\n<def>\n$$";
        var expectedTypeName = "function";
        var expectedPlatformName = "snowflake";
        var expectedObjectDumpMap =
                Map.of(
                        "args",
                        Map.of("propType", "map", "format", "<key> <value>", "delimiter", ","),
                        "packages",
                        Map.of("propType", "list", "format", "<package>", "delimiter", ","));

        var requestObjectDumpMap =
                Map.of(
                        "args",
                        PlatformMappingObjectDumpMapValue.builder()
                                .setPropType("map")
                                .setFormat("<key> <value>")
                                .setDelimiter(",")
                                .build(),
                        "packages",
                        PlatformMappingObjectDumpMapValue.builder()
                                .setPropType("list")
                                .setFormat("<package>")
                                .setDelimiter(",")
                                .build());
        PlatformMapping platformMapping =
                PlatformMapping.builder()
                        .setTypeName(expectedTypeName)
                        .setPlatformName(expectedPlatformName)
                        .setSyntax(expectedSyntax)
                        .setObjectDumpMap(requestObjectDumpMap)
                        .build();
        CreatePlatformMappingRequest request =
                CreatePlatformMappingRequest.builder().setPlatformMapping(platformMapping).build();

        UserDefinedPlatformMapping actualMapping =
                UserDefinedPlatformMapping.fromRequest(expectedTypeName, expectedPlatformName, request);

        assertThat(actualMapping.typeName()).isEqualTo(expectedTypeName);
        assertThat(actualMapping.platformName()).isEqualTo(expectedPlatformName);
        assertThat(actualMapping.templateSyntax()).isEqualTo(expectedSyntax);
        assertThat(actualMapping.additionalSyntaxPropsMap()).containsKey("args");
        assertThat(actualMapping.additionalSyntaxPropsMap().get("args").propType()).isEqualTo("map");
        assertThat(actualMapping.additionalSyntaxPropsMap().get("args").format()).isEqualTo("<key> <value>");
        assertThat(actualMapping.additionalSyntaxPropsMap().get("args").delimiter()).isEqualTo(",");
        assertThat(actualMapping.additionalSyntaxPropsMap()).hasSize(2);
    }

    @Test
    void toGenericRecord() {
        var expectedSyntax =
                "CREATE OR ALTER <type> <signature>\n    RETURNS <return_type>\n    LANGUAGE <language>\n    RUNTIME = <runtime>\n    HANDLER = '<name>'\n    AS $$\n<def>\n$$";
        var expectedTypeName = "function";
        var expectedPlatformName = "snowflake";
        var expectedObjectDumpMap =
                Map.of(
                        "args",
                        UserDefinedPlatformMapping.AdditionalSyntaxProps.builder()
                                .setPropType("map")
                                .setFormat("<key> <value>")
                                .setDelimiter(",")
                                .build(),
                        "packages",
                        UserDefinedPlatformMapping.AdditionalSyntaxProps.builder()
                                .setPropType("list")
                                .setFormat("<package>")
                                .setDelimiter(",")
                                .build());
        UserDefinedPlatformMapping actualMapping =
                UserDefinedPlatformMapping.builder(
                                expectedTypeName, expectedPlatformName, expectedSyntax, 1)
                        .setAdditionalSyntaxPropMap(expectedObjectDumpMap)
                        .setIcebergSchema(UserDefinedPlatformMapping.getIcebergSchema())
                        .build();

        GenericRecord genericRecord = actualMapping.toGenericRecord();

        assertThat(genericRecord.getField("uname")).isEqualTo(expectedTypeName);
        assertThat(genericRecord.getField("platform")).isEqualTo(expectedPlatformName);
        assertThat(genericRecord.getField("syntax")).isEqualTo(expectedSyntax);
        assertThat(genericRecord.getField("object_dump_map")).isInstanceOf(Map.class);
        assertThat((Map<?, ?>) genericRecord.getField("object_dump_map")).hasSize(2);
        assertThat(((Map<?, ?>) genericRecord.getField("object_dump_map")).get("args")).isInstanceOf(Record.class);
    }

    @Test
    void testGetSyntaxTupleList() {
        String syntax = "CREATE OR ALTER <type> <signature>\n    RETURNS <return_type>\n    LANGUAGE <language>\n    RUNTIME = <runtime>\n    HANDLER = '<name>'\n    AS $$\n<def>\n$$";
        String typeName = "function";
        String platformName = "snowflake";
        UserDefinedPlatformMapping platformMapping = UserDefinedPlatformMapping.builder().setTypeName(typeName)
                .setPlatformName(platformName)
                .setTemplateSyntax(syntax)
                .build();

        List<UserDefinedPlatformMapping.SyntaxTuple> syntaxList = platformMapping.getSyntaxTupleList();

        var first = syntaxList.getFirst();
        assertThat(first.placeholder()).isEqualTo("type");
        assertThat(first.prefix()).isEqualTo("CREATE OR ALTER ");
        assertThat(first.toString()).isEqualTo("CREATE OR ALTER type");
    }
}
