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

        assert actualMapping.typeName().equals(expectedTypeName);
        assert actualMapping.platformName().equals(expectedPlatformName);
        assert actualMapping.templateSyntax().equals(expectedSyntax);
        assert actualMapping.additionalSyntaxPropsMap().containsKey("args");
        assert actualMapping.additionalSyntaxPropsMap().get("args").propType().equals("map");
        assert actualMapping.additionalSyntaxPropsMap().get("args").format().equals("<key> <value>");
        assert actualMapping.additionalSyntaxPropsMap().get("args").delimiter().equals(",");
        assert actualMapping.additionalSyntaxPropsMap().size() == 2;
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

        assert genericRecord.getField("uname").equals(expectedTypeName);
        assert genericRecord.getField("platform").equals(expectedPlatformName);
        assert genericRecord.getField("syntax").equals(expectedSyntax);
        assert genericRecord.getField("object_dump_map") instanceof Map;
        assert ((Map<?,?>) genericRecord.getField("object_dump_map")).size() == 2;
        assert ((Map<?,?>) genericRecord.getField("object_dump_map")).get("args") instanceof Record;

    }

    @Test
    void testGetSyntaxMapping(){
        String syntax = "CREATE OR ALTER <type> <signature>\n    RETURNS <return_type>\n    LANGUAGE <language>\n    RUNTIME = <runtime>\n    HANDLER = '<name>'\n    AS $$\n<def>\n$$";
        String typeName = "function";
        String platformName = "snowflake";
        UserDefinedPlatformMapping platformMapping= UserDefinedPlatformMapping.builder().setTypeName(typeName)
                .setPlatformName(platformName)
                .setTemplateSyntax(syntax)
                .build();

        List<UserDefinedPlatformMapping.SyntaxTuple> syntaxList = platformMapping.getSyntaxTupleList();

        var first = syntaxList.getFirst();
        assert first.placeholder().equals("type");
        assert first.prefix().equals("CREATE OR ALTER ");
        assert first.toString().equals("CREATE OR ALTER type");

    }
}
