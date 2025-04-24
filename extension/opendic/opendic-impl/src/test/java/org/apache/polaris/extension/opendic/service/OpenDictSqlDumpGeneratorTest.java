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

package org.apache.polaris.extension.opendic.service;

import org.apache.polaris.extension.opendic.entity.UserDefinedEntity;
import org.apache.polaris.extension.opendic.entity.UserDefinedPlatformMapping;
import org.apache.polaris.extension.opendic.model.Statement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class OpenDictSqlDumpGeneratorTest {

    OpenDictSqlDumpGenerator openDictSqlDumpGenerator;
    UserDefinedPlatformMapping platformMappingSimple;
    UserDefinedPlatformMapping platformMappingComplex;
    List<UserDefinedEntity> entitiesSimple;
    List<UserDefinedEntity> entitiesComplex;


    @BeforeEach
    void setUp() {
        openDictSqlDumpGenerator = new OpenDictSqlDumpGenerator();
        entitiesComplex = List.of(
                UserDefinedEntity.builder()
                        .setName("foo")
                        .setObjectType("function")
                        .setEntityVersion(1)
                        .setProps(Map.of("args", Map.of("arg1", "int", "arg2", "int"),
                                "language", "python",
                                "runtime", "3.10",
                                "packages", List.of("numpy", "pandas"),
                                "def", String.join("\n",
                                        "def foo(arg1, arg2):",
                                        "       return arg1 + arg2"),
                                "comment", "foo function",
                                "handler", "foo",
                                "return_type", "int"
                        )).build());
        entitiesSimple = List.of(
                UserDefinedEntity.builder()
                        .setName("foo")
                        .setObjectType("function")
                        .setEntityVersion(1)
                        .setProps(Map.of("language", "python",
                                "runtime", "3.10",
                                "def", String.join("\n",
                                                "def foo():",
                                        "       print('Hello World')",
                                        "           return None"
                                ),
                                "comment", "foo function",
                                "handler", "foo",
                                "signature", "foo()",
                                "return_type", "void"
                        )).build());

        String syntaxSimple = String.join("\n",
                "CREATE OR ALTER <type> <signature>",
                "   RETURNS <return_type>",
                "   LANGUAGE <language>",
                "   RUNTIME = <runtime>",
                "   HANDLER = '<name>'",
                "   AS $$",
                "   <def>",
                "   $$");
        String typeNameSimple = "function";
        String platformNameSimple = "snowflake";
        platformMappingSimple = UserDefinedPlatformMapping.builder().setTypeName(typeNameSimple)
                .setPlatformName(platformNameSimple)
                .setTemplateSyntax(syntaxSimple)
                .build();

        String syntaxComplex = String.join("\n",
                "CREATE OR ALTER <type> <name>(<args>)",
                "   RETURNS <return_type>",
                "   LANGUAGE <language>",
                "   PACKAGES = (<packages>)",
                "   RUNTIME = <runtime>",
                "   HANDLER = '<name>'",
                "   AS $$",
                "   <def>",
                "   $$");

        Map<String, UserDefinedPlatformMapping.AdditionalSyntaxProps> additionalSyntaxPropsMap = Map.of(
                "args", UserDefinedPlatformMapping.AdditionalSyntaxProps.builder()
                        .setPropType("map")
                        .setFormat("<key> <value>")
                        .setDelimiter(", ")
                        .build(),
                "packages", UserDefinedPlatformMapping.AdditionalSyntaxProps.builder()
                        .setPropType("list")
                        .setFormat("'<item>'")
                        .setDelimiter(", ")
                        .build()
        );
        platformMappingComplex = UserDefinedPlatformMapping.builder().setTypeName(typeNameSimple)
                .setPlatformName(platformNameSimple)
                .setTemplateSyntax(syntaxComplex)
                .setAdditionalSyntaxPropMap(additionalSyntaxPropsMap)
                .build();
    }

    @Test
    void testEntityDumpSimple() {
        Statement expected = Statement.builder()
                .setDefinition(
                        String.join("\n",
                                "CREATE OR ALTER function foo()",
                                "   RETURNS void",
                                "   LANGUAGE python",
                                "   RUNTIME = 3.10",
                                "   HANDLER = 'foo'",
                                "   AS $$",
                                "   def foo():",
                                "       print('Hello World')",
                                "           return None",
                                "   $$")).build();

        Statement actual = openDictSqlDumpGenerator.entityDump(entitiesSimple.getFirst(),
                platformMappingSimple.templateSyntax(),
                platformMappingSimple.getSyntaxTupleList()
        );

        Assertions.assertEquals(actual.getDefinition(), expected.getDefinition());
    }

    @Test
    void testEntitiesComplex() {
        Statement expected = Statement.builder()
                .setDefinition(String.join(
                        "\n",
                        "CREATE OR ALTER function foo(arg1 int, arg2 int)",
                        "   RETURNS int",
                        "   LANGUAGE python",
                        "   PACKAGES = ('numpy', 'pandas')",
                        "   RUNTIME = 3.10",
                        "   HANDLER = 'foo'",
                        "   AS $$",
                        "   def foo(arg1, arg2):",
                        "       return arg1 + arg2",
                        "   $$"
                )).build();
        var syntaxTupleList = platformMappingComplex.getSyntaxTupleList();
        Statement actual = openDictSqlDumpGenerator.entityDump(entitiesComplex.getFirst(),
                platformMappingComplex.templateSyntax(),
                platformMappingComplex.getSyntaxTupleList(),
                platformMappingComplex.additionalSyntaxPropsMap()
        );

        Assertions.assertEquals(actual.getDefinition(), expected.getDefinition());
    }

    @Test
    void dumpStatements() {
    }
}