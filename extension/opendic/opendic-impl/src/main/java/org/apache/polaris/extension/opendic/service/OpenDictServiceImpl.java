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
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.secrets.UserSecretsManagerFactory;
import org.apache.polaris.extension.opendic.api.PolarisObjectsApiService;
import org.apache.polaris.extension.opendic.api.PolarisPlatformsApiService;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntity;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntitySchema;
import org.apache.polaris.extension.opendic.entity.UserDefinedPlatformMapping;
import org.apache.polaris.extension.opendic.model.*;
import org.apache.polaris.extension.opendic.persistence.IBaseRepository;
import org.apache.polaris.extension.opendic.persistence.IcebergRepository;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Concrete implementation of the Polaris API services
 */
@RequestScoped
public class OpenDictServiceImpl implements PolarisObjectsApiService, PolarisPlatformsApiService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenDictServiceImpl.class);
    private static final Marker OPENDIC_MARKER = MarkerFactory.getMarker("OPENDIC");

    private final RealmEntityManagerFactory entityManagerFactory;
    private final PolarisAuthorizer polarisAuthorizer;
    private final MetaStoreManagerFactory metaStoreManagerFactory;
    private final UserSecretsManagerFactory userSecretsManagerFactory;
    private final CallContext callContext;

    @Inject
    public OpenDictServiceImpl(
            RealmEntityManagerFactory entityManagerFactory,
            MetaStoreManagerFactory metaStoreManagerFactory,
            UserSecretsManagerFactory userSecretsManagerFactory,
            PolarisAuthorizer polarisAuthorizer,
            CallContext callContext
    ) {
        this.entityManagerFactory = entityManagerFactory;
        this.metaStoreManagerFactory = metaStoreManagerFactory;
        this.userSecretsManagerFactory = userSecretsManagerFactory;
        this.polarisAuthorizer = polarisAuthorizer;
        this.callContext = callContext;
        CallContext.setCurrentContext(callContext);
    }

    // Reuse the same helper method from PolarisServiceImpl
    private OpenDictService newAdminService(RealmContext realmContext, SecurityContext securityContext
    ) {
        AuthenticatedPolarisPrincipal authenticatedPrincipal =
                (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
        if (authenticatedPrincipal == null) {
            throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
        }
        // Create a new admin service with the right context
        PolarisEntityManager entityManager =
                entityManagerFactory.getOrCreateEntityManager(realmContext);
        PolarisMetaStoreManager metaStoreManager =
                metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
        UserSecretsManager userSecretsManager =
                userSecretsManagerFactory.getOrCreateUserSecretsManager(realmContext);
        IBaseRepository icebergRepository = new IcebergRepository();
        IOpenDictDumpGenerator openDictDumpGenerator = new OpenDictSqlDumpGenerator();
        return new OpenDictService(
                callContext,
                entityManager,
                metaStoreManager,
                userSecretsManager,
                securityContext,
                polarisAuthorizer,
                icebergRepository,
                openDictDumpGenerator);
    }

    /**
     * List all objects of UDO: {type}
     * Path: {@code GET /api/opendic/v1/objects/{type}}
     */
    @Override
    public Response listUdoObjects(String type, RealmContext realmContext, SecurityContext securityContext) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);

        List<Udo> udosOfType = adminService.listUdosOfType(type).stream().map(UserDefinedEntity::toUdo).toList();

        LOGGER.info(OPENDIC_MARKER, "Listing {}s: {}", type, udosOfType);
        return Response.status(Response.Status.OK)
                .entity(udosOfType)
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    /**
     * List all UDO types
     * Path: {@code GET /api/opendic/v1/objects}
     */
    @Override
    public Response listUdoTypes(RealmContext realmContext, SecurityContext securityContext) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);

        List<UdoSchema> schemaMap = adminService.listUdoTypes(realmContext, securityContext).stream()
                .map(UserDefinedEntitySchema::toUdoSchema)
                .toList();

        LOGGER.info(OPENDIC_MARKER, "Listed Types: {}", schemaMap);
        return Response.status(Response.Status.OK)
                .entity(schemaMap)
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    /**
     * Delete all UDOs of {@code type} and drop the schema.
     *
     * @param type The name/type of the UDO to delete
     */
    @Override
    public Response deleteUdo(String type, RealmContext realmContext, SecurityContext securityContext) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);
        var deleted = adminService.deleteUdoOfType(type);

        if (deleted) {
            LOGGER.info(OPENDIC_MARKER, "Deleted type: {}", type);
            return Response.status(Response.Status.OK)
                    .entity(Map.of("Deleted all objects of type", type))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } else {
            LOGGER.error(OPENDIC_MARKER, "Failed to delete type: {}", type);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("Failed to delete type. No such type", type))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }


    /**
     * Create a new UDO object of type {@code type}
     * Path: @code POST /api/opendic/v1/objects/{type}
     *
     * @param type    The type of the UDO to create
     * @param request The request containing the UDO object to create (type, name, jsonprops)
     */
    @Override
    public Response createUdo(String type, CreateUdoRequest request, RealmContext realmContext, SecurityContext securityContext
    ) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);
        Preconditions.checkNotNull(request.getUdo());
        LOGGER.info(OPENDIC_MARKER, "Creating open {} {} props: {}", type, request.getUdo().getName(), request.getUdo().getProps());
        try {
            UserDefinedEntity entity = UserDefinedEntity.fromRequest(type, request);
            UserDefinedEntity createdEntity = adminService.createUdo(entity);
            return Response.status(Response.Status.CREATED)
                    .entity(createdEntity)
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (IllegalArgumentException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to validate object {} against schema: {}. Error: {}", request.getUdo().getName(), type, e.getMessage(), e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("Failed to validate object against schema", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (AlreadyExistsException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to create UDO {}: {}", request.getUdo().getName(), e.getMessage(), e);
            return Response.status(Response.Status.CONFLICT)
                    .entity(Map.of("UDO already exists", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (IOException e) {
            LOGGER.error(OPENDIC_MARKER, "I/O error creating UDO {}: {}", request.getUdo().getName(), e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("I/O error", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    /**
     * Batch create UDO objects of type {@code type}
     * Path: {@code POST /api/opendic/v1/objects/{type}/batch}
     *
     * @param type The type of udos to batch create
     * @param udos The list of UDO objects to create
     */
    @Override
    public Response batchCreateUdo(String type, List<@Valid Udo> udos, RealmContext realmContext, SecurityContext securityContext) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);
        Preconditions.checkNotNull(udos);
        LOGGER.info(OPENDIC_MARKER, "Batch creating {} open {}s", udos.size(), type);

        var udosRequests = udos.stream()
                .map(udo -> CreateUdoRequest.builder().setUdo(udo).build())
                .toList();
        List<UserDefinedEntity> createEntities = udosRequests.stream()
                .map(createUdoRequest -> UserDefinedEntity.fromRequest(type, createUdoRequest))
                .toList();

        try {
            List<UserDefinedEntity> createdEntities = adminService.createUdos(type, createEntities);
            List<Udo> createdUdos = createdEntities.stream()
                    .map(UserDefinedEntity::toUdo)
                    .toList();
            return Response.status(Response.Status.CREATED)
                    .entity(createdUdos)
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (IllegalArgumentException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to validate object {} against schema. {}", type, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("Failed to validate object against schema", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (AlreadyExistsException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to create UDO {}: {}", type, e.getMessage(), e);
            return Response.status(Response.Status.CONFLICT)
                    .entity(Map.of("UDO already exists", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (IOException e) {
            LOGGER.error(OPENDIC_MARKER, "I/O error creating UDO {}: {}", type, e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("I/O error", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (NotAuthorizedException e) {
            LOGGER.error(OPENDIC_MARKER, "Not authorized to create UDO {}: {}", type, e.getMessage(), e);
            return Response.status(Response.Status.UNAUTHORIZED)
                    .entity(Map.of("Not authorized", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    /**
     * Define a new UDO type
     * Path: {@code POST /api/opendic/v1/objects}
     *
     * @param request The request containing the UDO type and its schema properties as json
     */
    @Override
    public Response defineUdo(DefineUdoRequest request, RealmContext realmContext, SecurityContext securityContext) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);
        Preconditions.checkNotNull(request.getUdoType());
        LOGGER.info(OPENDIC_MARKER, "Defining new UDO type: {}", request.getUdoType());

        try {
            UserDefinedEntitySchema schema = UserDefinedEntitySchema.fromRequest(request);
            UserDefinedEntitySchema createdSchema = adminService.defineSchema(schema);
            LOGGER.info(OPENDIC_MARKER, "Defined new udo type {}", request.getUdoType());

            return Response.status(Response.Status.CREATED)
                    .entity(createdSchema.toUdoSchema())
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (IllegalArgumentException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to create schema: {}", e.getMessage(), e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (AlreadyExistsException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to create schema {}: {}", request.getUdoType(), e.getMessage(), e);
            return Response.status(Response.Status.CONFLICT)
                    .entity(Map.of("message", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }


    /**
     * Update an existing UDO object of type {@code type}
     * Path: {@code PUT /api/opendic/v1/objects/{type}/{objectName}}
     *
     * @param type             The name of the UDO type to update
     * @param createUdoRequest The request containing the updated UDO object
     */
    @Override
    public Response updateUdo(String type, String objectName, CreateUdoRequest createUdoRequest, RealmContext realmContext, SecurityContext securityContext) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);
        Preconditions.checkNotNull(createUdoRequest.getUdo());
        Preconditions.checkArgument(type.equals(createUdoRequest.getUdo().getType()), "Type in request does not match type in path");
        Preconditions.checkArgument(objectName.equals(createUdoRequest.getUdo().getName()), "Name in request does not match name in path");
        LOGGER.info(OPENDIC_MARKER, "Updating open {} {} props: {}", type, createUdoRequest.getUdo().getName(), createUdoRequest.getUdo().getProps());
        try {
            UserDefinedEntity entity = UserDefinedEntity.fromRequest(type, createUdoRequest);
            UserDefinedEntity updatedEntity = adminService.updateUdo(type, objectName, entity);
            return Response.status(Response.Status.OK)
                    .entity(updatedEntity)
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (IllegalArgumentException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to validate object {} against schema: {}. Error: {}", createUdoRequest.getUdo().getName(), type, e.getMessage(), e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("Failed to validate object against schema", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (AlreadyExistsException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to update UDO {}: {}", createUdoRequest.getUdo().getName(), e.getMessage(), e);
            return Response.status(Response.Status.CONFLICT)
                    .entity(Map.of("UDO already exists", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (NoSuchElementException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to update UDO {}. No such element: {}", createUdoRequest.getUdo().getName(), e.getStackTrace());
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("No such element", "No such element"))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    /**
     * List all platform mappings for a specific object {@code type}
     * Path: {@code GET /api/opendic/v1/objects/{type}/platforms}
     *
     * @param type object type name.
     */
    @Override
    public Response listPlatformsForUdo(String type, RealmContext realmContext, SecurityContext securityContext) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);
        List<PlatformMapping> platforms = adminService.listMappingsForType(type).stream()
                .map(UserDefinedPlatformMapping::toPlatformMapping)
                .toList();

        LOGGER.info(OPENDIC_MARKER, "Listed platforms for {}: {}", type, platforms);
        return Response.status(Response.Status.OK)
                .entity(platforms)
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    /**
     * Get the platform mapping for a specific object {@code type} and {@code platform}
     * Path: {@code GET /api/opendic/v1/objects/{type}/platforms/{platform}}
     *
     * @param type     object type name.
     * @param platform the name of the platform
     */
    @Override
    public Response getPlatformMappingForUdo(String type, String platform, RealmContext realmContext, SecurityContext securityContext) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);
        try {
            UserDefinedPlatformMapping platformMapping = adminService.getPlatformMapping(type, platform);
            return Response.status(Response.Status.OK)
                    .entity(platformMapping.toPlatformMapping())
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (NotFoundException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to get platform mapping {}: {}", type, e.getMessage(), e);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("Platform mapping not found", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    /**
     * Get SQL dump of for a specific object {@code type} on a {@code platform}
     * Path: {@code GET /api/opendic/v1/objects/{type}/platforms/{platform}/pull}
     *
     * @param type     object type name.
     * @param platform the name of the platform
     */
    @Override
    public Response pullUdo(String type, String platform, RealmContext realmContext, SecurityContext securityContext) {
        LOGGER.info(OPENDIC_MARKER, "Pulling UDO {} for platform {}", type, platform);
        OpenDictService adminService = newAdminService(realmContext, securityContext);
        try {
            List<Statement> statements = adminService.pullDumpStatements(type, platform);
            return Response.status(Response.Status.OK)
                    .entity(statements)
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (NotFoundException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to pull UDO {}: {}", type, e.getMessage(), e);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("Platform mapping not found", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    /**
     * Define mew platform for a specific object {@code type}
     * Path: {@code POST /api/opendic/v1/objects/{type}/platforms/{platform}}
     *
     * @param type                         object type name.
     * @param platform                     the name of the platform
     * @param createPlatformMappingRequest the request to create the platform mapping. Contains template syntax and optional objectDumpMap - Primarily for collections
     */
    @Override
    public Response createPlatformMappingForUdo(String type, String platform, CreatePlatformMappingRequest
            createPlatformMappingRequest, RealmContext realmContext, SecurityContext securityContext) {
        LOGGER.info(OPENDIC_MARKER, "Creating platform mapping {} --> {} : {}", type, platform, createPlatformMappingRequest);
        OpenDictService adminService = newAdminService(realmContext, securityContext);
        Preconditions.checkNotNull(createPlatformMappingRequest.getPlatformMapping());
        UserDefinedPlatformMapping userdefinedPlatformMapping = UserDefinedPlatformMapping.fromRequest(type, platform, createPlatformMappingRequest);
        try {
            UserDefinedPlatformMapping created = adminService.createPlatformMapping(userdefinedPlatformMapping);
            return Response.status(Response.Status.CREATED)
                    .entity(created.toPlatformMapping())
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (IOException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to create platform mapping {}: {}", type, e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("Failed to create platform mapping", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    @Override
    public Response listMappingsForPlatform(String platform, RealmContext realmContext, SecurityContext
            securityContext) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);
        try {
            List<UserDefinedPlatformMapping> mappings = adminService.listMappingsForPlatform(platform);
            List<PlatformMapping> mappingsDto = mappings.stream().map(UserDefinedPlatformMapping::toPlatformMapping).toList();
            LOGGER.info(OPENDIC_MARKER, "Listed mappings for platform {}: {}", platform, mappingsDto);
            return Response.status(Response.Status.OK)
                    .entity(mappingsDto)
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (NotFoundException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to list mappings for platform {}: {}", platform, e.getMessage(), e);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("Platform not found", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    @Override
    public Response listPlatforms(RealmContext realmContext, SecurityContext securityContext) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);

        List<PlatformMappings> schemaMap = adminService.listPlatforms(realmContext, securityContext);

        LOGGER.info(OPENDIC_MARKER, "Listed Platforms: {}", schemaMap);
        return Response.status(Response.Status.OK)
                .entity(schemaMap)
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    @Override
    public Response deleteMappingsForPlatform(String platform, RealmContext realmContext, SecurityContext
            securityContext) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);
        boolean deleted = adminService.deleteMappingsForPlatform(platform);
        if (deleted) {
            LOGGER.info(OPENDIC_MARKER, "Deleted mappings for platform: {}", platform);
            return Response.status(Response.Status.OK)
                    .entity(Map.of("Deleted all mappings for platform", platform))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } else {
            LOGGER.error(OPENDIC_MARKER, "Failed to delete mappings for platform: {}", platform);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("No mappings for platform snowflake found", platform))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    /**
     * Pull the dump statements for a specific platform.
     * Path: {@code GET /api/opendic/v1/platforms/{platform}/pull}
     *
     * @param platform The name of the platform
     * @return Json response with a list of dump statements for the platform.
     */
    @Override

    public Response pullPlatform(String platform, RealmContext realmContext, SecurityContext securityContext) {
        OpenDictService adminService = newAdminService(realmContext, securityContext);
        List<Statement> statements = adminService.pullPlatformStatements(platform);
        try {
            return Response.status(Response.Status.OK)
                    .entity(statements)
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (NotFoundException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to pull platform {}: {}", platform, e.getMessage(), e);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("Platform not found", e.getMessage(), "platform", platform, "trace", e.getStackTrace()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }
}
