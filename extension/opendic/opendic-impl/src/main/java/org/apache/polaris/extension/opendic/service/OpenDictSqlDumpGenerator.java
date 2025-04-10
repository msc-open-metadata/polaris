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
import org.apache.polaris.extension.opendic.entity.UserDefinedEntity;
import org.apache.polaris.extension.opendic.entity.UserDefinedPlatformMapping;
import org.apache.polaris.extension.opendic.model.Statement;

import java.util.List;

@ApplicationScoped
public class OpenDictSqlDumpGenerator implements IOpenDictDumpGenerator {
    @Override
    public Statement recordDump(Record udoRecord, String syntaxMap, UserDefinedPlatformMapping.AdditionalSyntaxProps additionalSyntaxProps) {
        return null;
    }

    @Override
    public Statement recordDump(Record udoRecord, String syntaxMap) {
        return null;
    }

    @Override
    public List<Statement> dumpStatements(List<UserDefinedEntity> entities, UserDefinedPlatformMapping userDefinedPlatformMapping) {
        return List.of();
    }
}
