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

import org.apache.polaris.extension.opendic.entity.UserDefinedPlatformMapping;
import org.apache.polaris.extension.opendic.model.Statement;
import org.apache.iceberg.data.Record;
import java.util.List;
import java.util.Map;

public interface IOpenDictDumpGenerator {

    static final String OBJECT_NAME_KEY = "name";
    static final String OBJECT_TYPE_KEY = "type";

    Statement recordDump(Record udoEntity, String syntaxMap, List<UserDefinedPlatformMapping.SyntaxMapEntry> syntaxReplacementList, Map<String, UserDefinedPlatformMapping.AdditionalSyntaxProps> additionalSyntaxProps);

    Statement recordDump(Record udoEntity, String syntaxMap, List<UserDefinedPlatformMapping.SyntaxMapEntry> syntaxReplacementList);

    /**
     * Generate a list of program statements for a specific platform using the syntax defined by {@code userDefinedPlatformMapping}
     *
     * @param entities                   The UDO entities. Example: [(function, foo), (function, bar), (function, baz)]
     * @param userDefinedPlatformMapping The platform mapping defining the object type, platform, and template syntax.
     * @return List of executable statements.
     */
    List<Statement> dumpStatements(List<Record> entities, UserDefinedPlatformMapping userDefinedPlatformMapping);
}
