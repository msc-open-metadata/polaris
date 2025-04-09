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

import java.util.Map;
import org.apache.polaris.extension.opendic.model.CreatePlatformMappingRequest;
import org.apache.polaris.extension.opendic.model.PlatformMapping;
import org.apache.polaris.extension.opendic.model.PlatformMappingObjectDumpMapValue;
import org.junit.jupiter.api.Test;

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
    assert actualMapping.objectDumpMap().containsKey("args");
    assert actualMapping.objectDumpMap().containsKey("packages");
    assert actualMapping.objectDumpMap().get("args").get("propType").equals("map");
    assert actualMapping.objectDumpMap().get("args").get("format").equals("<key> <value>");
    assert actualMapping.objectDumpMap().get("args").get("delimiter").equals(",");
  }

  @Test
  void toObjMap() {
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
    UserDefinedPlatformMapping actualMapping =
        UserDefinedPlatformMapping.builder(
                expectedTypeName, expectedPlatformName, expectedSyntax, 1)
            .setObjectDumpMap(expectedObjectDumpMap)
            .build();

    var platformMappingAsObjMap = actualMapping.toObjMap();

    assert platformMappingAsObjMap.containsKey("type");
    assert platformMappingAsObjMap.get("type").equals(expectedTypeName);
    assert platformMappingAsObjMap.containsKey("platform");
    assert platformMappingAsObjMap.get("platform").equals(expectedPlatformName);
    assert platformMappingAsObjMap.containsKey("syntax");
    assert platformMappingAsObjMap.get("syntax").equals(expectedSyntax);
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
            Map.of("propType", "map", "format", "<key> <value>", "delimiter", ","),
            "packages",
            Map.of("propType", "list", "format", "<package>", "delimiter", ","));
    UserDefinedPlatformMapping actualMapping =
        UserDefinedPlatformMapping.builder(
                expectedTypeName, expectedPlatformName, expectedSyntax, 1)
            .setObjectDumpMap(expectedObjectDumpMap)
            .setIcebergSchema(UserDefinedPlatformMapping.getIcebergSchema())
            .build();

    var genericRecord = actualMapping.toGenericRecord();

    assert genericRecord != null;
    assert genericRecord.getField("type").equals(expectedTypeName);
    assert genericRecord.getField("platform").equals(expectedPlatformName);
    assert genericRecord.getField("syntax").equals(expectedSyntax);
  }
}
