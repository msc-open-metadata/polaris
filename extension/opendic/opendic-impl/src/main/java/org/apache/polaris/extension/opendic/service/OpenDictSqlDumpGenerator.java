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

import com.google.common.base.Preconditions;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntity;
import org.apache.polaris.extension.opendic.entity.UserDefinedPlatformMapping;
import org.apache.polaris.extension.opendic.entity.UserDefinedPlatformMapping.AdditionalSyntaxProps;
import org.apache.polaris.extension.opendic.entity.UserDefinedPlatformMapping.SyntaxTuple;
import org.apache.polaris.extension.opendic.model.Statement;

import java.util.List;
import java.util.Map;

@ApplicationScoped
public class OpenDictSqlDumpGenerator implements IOpenDictDumpGenerator {


    /**
     * Generate a list of program statements for a specific platform using the syntax defined by {@code userDefinedPlatformMapping}. Supports complex objects.
     *
     * @param udoRecord             The record containing the object entity.
     * @param syntax                The template syntax string from a platform mapping.
     * @param syntaxReplacementList The syntax replacement datastructure created from the platform mapping efficiently building statements from a entity and
     * @param additionalSyntaxProps The additional syntax properties for more complex objects like list or map.
     */
    @Override
    public Statement entityDump(UserDefinedEntity udoRecord, String syntax, List<SyntaxTuple> syntaxReplacementList, Map<String, AdditionalSyntaxProps> additionalSyntaxProps) {
        StringBuilder resultString = new StringBuilder();

        for (SyntaxTuple entry : syntaxReplacementList) {
            String placeholder = entry.placeholder();
            String prefix = entry.prefix();

            String replacement;

            switch (placeholder) {
                case null -> throw new IllegalArgumentException("Placeholder cannot be null");
                case "" -> replacement = prefix;
                case "type" -> replacement = prefix + udoRecord.typeName();
                case "name" -> replacement = prefix + udoRecord.objectName();
                default -> {
                    if (additionalSyntaxProps.containsKey(placeholder)) {
                        Object obj = udoRecord.props().get(placeholder);
                        replacement = prefix + handleSpecialObjectSyntax(obj, additionalSyntaxProps.get(placeholder));
                    } else {
                        String recordValue = String.valueOf(udoRecord.props().get(placeholder));
                        replacement = prefix + recordValue;
                    }
                }
            }
            resultString.append(replacement);
        }
        return new Statement(resultString.toString());
    }


    /**
     * Generate a list of program statements for a specific platform using the syntax defined by {@code userDefinedPlatformMapping}. No complex objects.
     */
    @Override
    public Statement entityDump(UserDefinedEntity udoRecord, String syntax, List<SyntaxTuple> syntaxReplacementList) {
        StringBuilder resultString = new StringBuilder();

        for (SyntaxTuple entry : syntaxReplacementList) {
            String placeholder = entry.placeholder();
            String prefix = entry.prefix();

            String replacement;
            switch (placeholder) {
                case null -> throw new IllegalArgumentException("Placeholder cannot be null");
                case "" -> replacement = prefix;
                case "type" -> replacement = prefix + udoRecord.typeName();
                case "name" -> replacement = prefix + udoRecord.objectName();
                default -> {
                    String recordValue = String.valueOf(udoRecord.props().get(placeholder));
                    replacement = prefix + recordValue;
                }

            }
            resultString.append(replacement);
        }
        return new Statement(resultString.toString());
    }

    @Override
    public List<Statement> dumpStatements(List<UserDefinedEntity> entities, UserDefinedPlatformMapping userDefinedPlatformMapping) {
        return entities.stream().map(entity -> entityDump(entity, userDefinedPlatformMapping.templateSyntax(), userDefinedPlatformMapping.getSyntaxTupleList(), userDefinedPlatformMapping.additionalSyntaxPropsMap())).toList();
    }

    private String handleSpecialObjectSyntax(Object complexObj, AdditionalSyntaxProps additionalSyntaxProps) {
        Preconditions.checkNotNull(complexObj, "Cannot handle syntax for null obiect");
        switch (complexObj) {
            case Map<?, ?> map -> {
                Preconditions.checkArgument(additionalSyntaxProps.propType().equalsIgnoreCase("map"), "Expected a map type for additional syntax properties");
                StringBuilder result = new StringBuilder();

                int idx = 0;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    String key = String.valueOf(entry.getKey());
                    String value = String.valueOf(entry.getValue());
                    String templateFormat = additionalSyntaxProps.format();
                    String replacementString = templateFormat.replace("<key>", key).replace("<value>", value);
                    // Dont add the last delimiter
                    if (idx++ < map.size() - 1) {
                        result.append(replacementString).append(additionalSyntaxProps.delimiter());
                    } else {
                        result.append(replacementString);
                    }
                }
                return result.toString();
            }
            case List<?> list -> {
                Preconditions.checkArgument(additionalSyntaxProps.propType().equalsIgnoreCase("list"), "Expected a list type for additional syntax properties");
                StringBuilder result = new StringBuilder();
                int idx = 0;
                for (Object item : list) {
                    String itemStr = String.valueOf(item);
                    String templateFormat = additionalSyntaxProps.format();
                    String replacementString = templateFormat.replace("<item>", itemStr);
                    if (idx++ < list.size() - 1) {
                        result.append(replacementString).append(additionalSyntaxProps.delimiter());
                    } else {
                        result.append(replacementString);
                    }
                }
                return result.toString();
            }
            default ->
                    throw new UnsupportedOperationException("Unsupported complex object type: " + complexObj + "for " + additionalSyntaxProps.propType());
        }
    }
}
