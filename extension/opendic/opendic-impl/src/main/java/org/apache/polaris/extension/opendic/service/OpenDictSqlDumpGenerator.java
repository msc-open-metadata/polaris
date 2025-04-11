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

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.iceberg.data.Record;
import org.apache.polaris.extension.opendic.entity.UserDefinedPlatformMapping;
import org.apache.polaris.extension.opendic.entity.UserDefinedPlatformMapping.AdditionalSyntaxProps;
import org.apache.polaris.extension.opendic.entity.UserDefinedPlatformMapping.SyntaxMapEntry;
import org.apache.polaris.extension.opendic.model.Statement;

import java.util.List;
import java.util.Map;

@ApplicationScoped
public class OpenDictSqlDumpGenerator implements IOpenDictDumpGenerator {
    @Override
    public Statement recordDump(Record udoRecord, String syntax, List<SyntaxMapEntry> syntaxReplacementList, Map<String, AdditionalSyntaxProps> additionalSyntaxProps) {
        String resultString = "";

        for (SyntaxMapEntry entry : syntaxReplacementList) {
            String placeholder = entry.placeholder();
            String prefix = entry.prefix();

            // any there special syntax props? Example: map, list.
            if (additionalSyntaxProps.containsKey(placeholder)) {

            }
        }
        return new Statement(resultString);
    }

    @Override
    public Statement recordDump(Record udoRecord, String syntax, List<SyntaxMapEntry> syntaxReplacementList) {
        StringBuilder resultString = new StringBuilder();

        for (SyntaxMapEntry entry : syntaxReplacementList) {
            String placeholder = entry.placeholder();
            String prefix = entry.prefix();
            String recordValue = !placeholder.isEmpty() ? udoRecord.getField(placeholder).toString() : "";
            String replacement = prefix + recordValue;
            resultString.append(replacement);
        }
        return new Statement(resultString.toString());
    }

    @Override
    public List<Statement> dumpStatements(List<Record> entities, UserDefinedPlatformMapping userDefinedPlatformMapping) {
        return entities.stream().map(entity -> recordDump(entity, userDefinedPlatformMapping.templateSyntax(), userDefinedPlatformMapping.getSyntaxMap(), userDefinedPlatformMapping.additionalSyntaxPropsMap())).toList();
    }

    private String handleSpecialObjectSyntax(Object field, AdditionalSyntaxProps additionalSyntaxProps) {
        if (field instanceof List<?> list) {
            return handleListSyntax(list, additionalSyntaxProps);
        } else if (field instanceof Map<?, ?> map) {
            return handleMapSyntax(map, additionalSyntaxProps);
        } else {
            throw new UnsupportedOperationException("Unsupported field type for special syntax: " + field.getClass());
        }
    }

    private String handleMapSyntax(Map<?, ?> map, AdditionalSyntaxProps additionalSyntaxProps) {
        return null;
    }

    private String handleListSyntax(List<?> list, AdditionalSyntaxProps additionalSyntaxProps) {
        return null;
    }

}
