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

import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;

import java.util.HashMap;

/**
 * Represents an instance of a user-defined entity type.
 * This allows for the creation and management of custom entity instances.
 */
public class UserDefinedEntity extends PolarisBaseEntity {
    private final UserDefinedEntityType entityType;

    /**
     * Private constructor to ensure instances are only created via factory methods.
     */
    private UserDefinedEntity(UserDefinedEntityType entityType) {
        this.entityType = entityType;
    }

    /**
     * Creates a new user-defined entity of the specified type.
     *
     * @param entityType The entity type definition
     * @param catalogId  The catalog ID
     * @param id         The entity ID
     * @param parentId   The parent entity ID
     * @param name       The entity name
     * @return A new UserDefinedEntity instance
     */
    public static UserDefinedEntity create(
            UserDefinedEntityType entityType,
            long catalogId,
            long id,
            long parentId,
            String name) {

        // Create a new entity
        UserDefinedEntity entity = new UserDefinedEntity(entityType);
        entity.setCatalogId(catalogId);
        entity.setId(id);
        entity.setTypeCode(PolarisEntityType.TABLE_LIKE.getCode());
        entity.setSubTypeCode(PolarisEntitySubType.ANY_SUBTYPE.getCode());
        entity.setParentId(parentId);
        entity.setName(name);

        // Set timestamps
        long currentTime = System.currentTimeMillis();
        entity.setCreateTimestamp(currentTime);
        entity.setLastUpdateTimestamp(currentTime);
        entity.setDropTimestamp(0);
        entity.setPurgeTimestamp(0);
        entity.setToPurgeTimestamp(0);

        // Initialize empty properties
        entity.setPropertiesAsMap(new HashMap<>());
        entity.setInternalPropertiesAsMap(new HashMap<>());

        // Set versions
        entity.setEntityVersion(42);
        entity.setGrantRecordsVersion(42);

        return entity;
    }

    /**
     * Creates a UserDefinedEntity from an existing PolarisBaseEntity.
     *
     * @param baseEntity The base entity to convert
     * @param entityType The entity type definition
     * @return A UserDefinedEntity instance
     * @throws IllegalArgumentException if the entity type doesn't match or validation fails
     */
    public static UserDefinedEntity fromBaseEntity(
            PolarisBaseEntity baseEntity,
            UserDefinedEntityType entityType) {

        if (baseEntity.getTypeCode() != PolarisEntityType.TABLE_LIKE.getCode()) {
            throw new IllegalArgumentException(
                    "Cannot convert to UserDefinedEntity: not a USER_DEFINED(TABLE_LIKE) type entity");
        }

        UserDefinedEntity entity = new UserDefinedEntity(entityType);

        // Copy fields from the base entity
        entity.setCatalogId(baseEntity.getCatalogId());
        entity.setId(baseEntity.getId());
        entity.setTypeCode(baseEntity.getTypeCode());
        entity.setSubTypeCode(baseEntity.getSubTypeCode());
        entity.setParentId(baseEntity.getParentId());
        entity.setName(baseEntity.getName());
        entity.setCreateTimestamp(baseEntity.getCreateTimestamp());
        entity.setDropTimestamp(baseEntity.getDropTimestamp());
        entity.setPurgeTimestamp(baseEntity.getPurgeTimestamp());
        entity.setToPurgeTimestamp(baseEntity.getToPurgeTimestamp());
        entity.setLastUpdateTimestamp(baseEntity.getLastUpdateTimestamp());
        entity.setProperties(baseEntity.getProperties());
        entity.setInternalProperties(baseEntity.getInternalProperties());
        entity.setEntityVersion(baseEntity.getEntityVersion());
        entity.setGrantRecordsVersion(baseEntity.getGrantRecordsVersion());

        return entity;
    }

    /**
     * Gets the entity type definition for this entity.
     *
     * @return The entity type
     */
    public UserDefinedEntityType getEntityType() {
        return entityType;
    }
}