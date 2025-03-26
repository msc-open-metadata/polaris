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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.extension.opendic.model.Udo;

import java.util.Map;

/**
 * Represents an instance of a user-defined entity type.
 * This allows for the creation and management of custom entity instances.
 */
public class UserDefinedEntity extends PolarisEntity {

    public static final String TYPE_SCHEMA = "type-schema";
    public static final String OBJECT = "object-definition";

    UserDefinedEntity(PolarisBaseEntity sourceEntity) {
        super(sourceEntity);
    }

    public static UserDefinedEntity of(PolarisBaseEntity sourceEntity) {
        if (sourceEntity != null) {
            return new UserDefinedEntity(sourceEntity);
        }
        return null;
    }

    public static UserDefinedEntity fromUdo(Udo udo) {
        Preconditions.checkNotNull(udo);
        //TODO find schema from name.
        return new Builder(udo.getName(), udo.getType())
                .setObjectDefinition(udo.getProps().toString())
                .setInternalProperties(Map.of("parent-namespace", "SYSTEM"))
                .build();
    }

    @JsonIgnore
    public String getTypeSchema() {
        Preconditions.checkArgument(
                getPropertiesAsMap().containsKey(TYPE_SCHEMA),
                "Invalid user-defined entity: type schema must exist");
        return getPropertiesAsMap().get(TYPE_SCHEMA);
    }

    @JsonIgnore
    public String getObjectDefinition() {
        return getPropertiesAsMap().get(OBJECT);
    }

    public static class Builder extends PolarisEntity.BaseBuilder<UserDefinedEntity, Builder> {
        public Builder(String entityName, String typeSchema) {
            super();
            setType(PolarisEntityType.ICEBERG_TABLE_LIKE);
            setName(entityName);
            setTypeSchema(typeSchema);
        }

        public Builder(UserDefinedEntity original) {
            super(original);
        }

        @Override
        public UserDefinedEntity build() {
            Preconditions.checkArgument(
                    properties.containsKey(TYPE_SCHEMA), "Type schema must be specified");

            return new UserDefinedEntity(buildBase());
        }

        public Builder setTypeCode(int typeCode) {
            properties.put(TYPE_SCHEMA, String.valueOf(typeCode));
            return this;
        }

        public Builder setTypeSchema(String typeSchema) {
            Preconditions.checkArgument(typeSchema != null, "Type schema must be specified");
            properties.put(TYPE_SCHEMA, typeSchema);
            return this;
        }

        public Builder setObjectDefinition(String objectDefinition) {
            properties.put(OBJECT, objectDefinition);
            return this;
        }
    }
}