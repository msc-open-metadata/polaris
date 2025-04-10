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

import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntity;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntitySchema;
import org.apache.polaris.extension.opendic.entity.UserDefinedPlatformMapping;
import org.apache.polaris.extension.opendic.model.PlatformMapping;
import org.apache.polaris.extension.opendic.model.PlatformMappings;
import org.apache.polaris.extension.opendic.model.Statement;
import org.apache.polaris.extension.opendic.model.Udo;
import org.apache.polaris.extension.opendic.persistence.IBaseRepository;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OpenDictService extends PolarisAdminService {

    // Initialized in the authorize methods.
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OpenDictService.class);
    private static final Marker OPENDIC_MARKER = MarkerFactory.getMarker("OPENDIC");
    private static final Namespace NAMESPACE = Namespace.of("SYSTEM");
    private static final Namespace PLATFORM_MAPPINGS_NAMESPACE = Namespace.of("SYSTEM", "PLATFORM_MAPPINGS");
    private final PolarisResolutionManifest resolutionManifest = null;
    private final IBaseRepository icebergRepository;
    private final IOpenDictDumpGenerator openDictDumpGenerator;

    public OpenDictService(
            @NotNull CallContext callContext,
            @NotNull PolarisEntityManager entityManager,
            @NotNull PolarisMetaStoreManager metaStoreManager,
            @NotNull SecurityContext securityContext,
            @NotNull PolarisAuthorizer authorizer,
            @NotNull IBaseRepository icebergRepository,
            @NotNull IOpenDictDumpGenerator openDictDumpGenerator
    ) {
        super(callContext, entityManager, metaStoreManager, securityContext, authorizer);
        this.icebergRepository = icebergRepository;
        this.openDictDumpGenerator = openDictDumpGenerator;
    }

    public String defineSchema(UserDefinedEntitySchema schema) throws AlreadyExistsException {
        return icebergRepository.createTable(NAMESPACE, schema.typeName(), UserDefinedEntitySchema.getIcebergSchema(schema));
    }

    public Map<String, String> listUdoTypes(RealmContext realmContext, SecurityContext securityContext) {
        return icebergRepository.listTablesAsStringMap(NAMESPACE);
    }

    public String createUdo(UserDefinedEntity entity) throws IOException, AlreadyExistsException {
        var schema = icebergRepository.readTableSchema(NAMESPACE, entity.typeName());
        var genericRecord = icebergRepository.createGenericRecord(schema, entity.toMap());
        icebergRepository.insertRecord(NAMESPACE, entity.typeName(), genericRecord);
        return genericRecord.toString();
    }

    // Modified to return a list of Udos instead of a list of string - for consistency with openAPI spec components
    public List<Udo> listUdosOfType(String typeName) {
        var icebergSchema = icebergRepository.readTableSchema(NAMESPACE, typeName);
        try {
            List<Record> records = icebergRepository.readRecords(NAMESPACE, typeName);
            return records.stream()
                    .map(record -> UserDefinedEntity.fromRecord(record, typeName).toUdo())
                    .toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public boolean deleteUdoOfType(String typeName) {
        return icebergRepository.dropTable(NAMESPACE, typeName);
    }


    public String createPlatformMapping(UserDefinedPlatformMapping mappingEntity) throws IOException {
        var schema = mappingEntity.icebergSchema();
        icebergRepository.createTableIfNotExists(PLATFORM_MAPPINGS_NAMESPACE, mappingEntity.platformName(), schema);
        var genericRecord = mappingEntity.toGenericRecord();
        icebergRepository.insertRecord(PLATFORM_MAPPINGS_NAMESPACE, mappingEntity.platformName(), genericRecord);
        return genericRecord.toString();
    }

    public List<UserDefinedPlatformMapping> listMappingsForType(String typeName) {
        var tableIds = icebergRepository.listTables(PLATFORM_MAPPINGS_NAMESPACE);
        List<Record> matchedRecords = new ArrayList<>();
        try {
            for (var tableId : tableIds) {
                if (icebergRepository.containsRecordWithId(tableId, UserDefinedPlatformMapping.ID_COLUMN, typeName)) {
                    matchedRecords.add(icebergRepository.readRecordWithId(tableId, UserDefinedPlatformMapping.ID_COLUMN, typeName));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return matchedRecords.stream().map(UserDefinedPlatformMapping::fromRecord).toList();

    }

    public UserDefinedPlatformMapping getPlatformMapping(String typeName, String platformName) {
        try {
            Record record = icebergRepository.readRecordWithId(PLATFORM_MAPPINGS_NAMESPACE, platformName, UserDefinedPlatformMapping.ID_COLUMN, typeName);
            return UserDefinedPlatformMapping.fromRecord(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Statement> pullDumpStatements(String typeName, String platformName) throws IOException {
        List<UserDefinedEntity> entities = icebergRepository.readRecords(NAMESPACE, typeName).stream()
                .map(record -> UserDefinedEntity.fromRecord(record, typeName))
                .toList();

        // example: readRecordWithId(SYSTEM.PLATFORM_MAPPINGS, snowflake, "uname", function)
        UserDefinedPlatformMapping platformMapping = UserDefinedPlatformMapping.fromRecord(
                icebergRepository.readRecordWithId(PLATFORM_MAPPINGS_NAMESPACE,
                        platformName,
                        UserDefinedPlatformMapping.ID_COLUMN,
                        typeName));
        return openDictDumpGenerator.dumpStatements(entities, platformMapping);
    }

    // Platform API methods --------------------------------------------------------------------------------------------
    public boolean deleteMappingsForPlatform(String platformName) {
        return icebergRepository.dropTable(PLATFORM_MAPPINGS_NAMESPACE, platformName);
    }

    public List<PlatformMappings> listPlatforms(RealmContext realmContext, SecurityContext securityContext) {
        var platformTableIds = icebergRepository.listTables(PLATFORM_MAPPINGS_NAMESPACE);
        List<PlatformMappings> result = new ArrayList<>();
        try {
            for (var tableId : platformTableIds) {
                List<Record> records = icebergRepository.readRecords(tableId);
                List<PlatformMapping> mappings = records.stream()
                        .map(UserDefinedPlatformMapping::fromRecord)
                        .map(UserDefinedPlatformMapping::toPlatformMapping)
                        .toList();
                PlatformMappings platformMappings = PlatformMappings.builder()
                        .setPlatformMappings(mappings)
                        .setPlatformName(tableId.name())
                        .build();
                result.add(platformMappings);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public List<String> listMappingsForPlatform(String platformName) {
        try {
            return icebergRepository.readRecords(PLATFORM_MAPPINGS_NAMESPACE, platformName)
                    .stream()
                    .map(Record::toString)
                    .toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
