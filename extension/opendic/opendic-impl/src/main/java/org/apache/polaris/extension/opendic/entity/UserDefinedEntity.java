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

import com.google.common.base.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.polaris.extension.opendic.model.CreateUdoRequest;
import org.apache.polaris.extension.opendic.model.Udo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public record UserDefinedEntity(String typeName,
                                String objectName,
                                Map<String, Object> props,
                                OffsetDateTime createdTimeStamp,
                                OffsetDateTime lastUpdatedTimeStamp,
                                int entityVersion) {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDefinedEntity.class);
    public static final String ID_COLUMN = "uname";

    public static UserDefinedEntity fromRequest(String typeName, CreateUdoRequest request) {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(request.getUdo());
        Preconditions.checkArgument(!request.getUdo().getName().isEmpty(), "Name cannot be empty");
        Preconditions.checkArgument(!typeName.isEmpty(), "Type cannot be empty");
        Preconditions.checkNotNull(request.getUdo().getProps());
        Preconditions.checkArgument(!request.getUdo().getProps().isEmpty(), "Must define at least 1 property");
        String objectName = request.getUdo().getName();
        return new Builder(typeName, objectName, 1)
                .setProps(request.getUdo().getProps())
                .build();

    }

    public static Builder builder(String typeName, String objectName, int entityVersion) {
        return new Builder(typeName, objectName, entityVersion);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static UserDefinedEntity fromRecord(Record record, String typeName) {
        Preconditions.checkNotNull(record);
        Preconditions.checkNotNull(typeName);
        Schema icebergSchema = record.struct().asSchema();
        Preconditions.checkNotNull(icebergSchema);

        String name = String.valueOf(record.getField("uname"));
        OffsetDateTime createdTimestamp = OffsetDateTime.parse(record.getField("created_time").toString());
        OffsetDateTime lastUpdatedTimestamp = OffsetDateTime.parse(record.getField("last_updated_time").toString());
        int entityVersion = (int) record.getField("entity_version");


        // Dynamically build props from the schema
        Map<String, Object> props = new HashMap<>();
        for (Types.NestedField field : icebergSchema.columns()) {
            String fieldName = field.name();
            if (!Set.of("uname", "created_time", "last_updated_time", "entity_version").contains(fieldName)) {
                Object value = record.getField(fieldName);
                if (value != null) {
                    props.put(fieldName, value);
                }
            }
        }
        return UserDefinedEntity.builder(typeName, name, entityVersion)
                .setProps(props)
                .setCreateTimestamp(createdTimestamp)
                .setLastUpdateTimestamp(lastUpdatedTimestamp)
                .setEntityVersion(entityVersion)
                .build();
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("uname", objectName);
        map.putAll(props);
        map.put("created_time", createdTimeStamp.toString());
        map.put("last_updated_time", lastUpdatedTimeStamp.toString());
        map.put("entity_version", entityVersion);
        return map;
    }

    // we check typename and object name when we build the UserDefinedObject, but no checks for props, timestamps or version
    public Udo toUdo() {
        return Udo.builder(typeName, objectName)
                .setProps(props != null ? props : new HashMap<>())
                .setCreatedTimestamp(createdTimeStamp.toString())
                .setLastUpdatedTimestamp(lastUpdatedTimeStamp.toString())
                .setEntityVersion(entityVersion)
                .build();
    }

    public static class Builder {
        private final Map<String, Object> props = new HashMap<>();
        private String typeName;
        private String objectName;
        private OffsetDateTime createdTimeStamp = OffsetDateTime.now();
        private OffsetDateTime lastUpdatedTimeStamp = OffsetDateTime.now();
        private int entityVersion;

        public Builder(String typeName, String objectName, int entityVersion) {
            this.typeName = typeName;
            this.objectName = objectName;
            this.entityVersion = entityVersion;
        }

        public Builder() {
        }

        public Builder setName(String objectName) {
            this.objectName = objectName;
            return this;
        }

        public Builder setObjectType(String typeName) {
            this.typeName = typeName;
            return this;
        }

        public Builder setProps(Map<String, Object> props) {
            this.props.putAll(props);
            return this;
        }

        public Builder setCreateTimestamp(OffsetDateTime createdTimeStamp) {
            this.createdTimeStamp = createdTimeStamp;
            return this;
        }

        public Builder setLastUpdateTimestamp(OffsetDateTime lastUpdatedTimeStamp) {
            this.lastUpdatedTimeStamp = lastUpdatedTimeStamp;
            return this;
        }

        public Builder setEntityVersion(int entityVersion) {
            this.entityVersion = entityVersion;
            return this;
        }


        public UserDefinedEntity build() {
            Preconditions.checkNotNull(typeName);
            Preconditions.checkNotNull(objectName);
            Preconditions.checkNotNull(props);
            Preconditions.checkNotNull(createdTimeStamp);
            return new UserDefinedEntity(typeName, objectName, props, createdTimeStamp, lastUpdatedTimeStamp, entityVersion);
        }

    }


}
