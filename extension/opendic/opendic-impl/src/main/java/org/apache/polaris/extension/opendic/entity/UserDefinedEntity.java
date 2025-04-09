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
import org.apache.hadoop.util.Time;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.polaris.extension.opendic.model.Udo;
import org.apache.polaris.extension.opendic.model.CreateUdoRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.iceberg.data.Record;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public record UserDefinedEntity(String typeName,
                                String objectName,
                                Map<String, Object> props,
                                long createdTimeStamp,
                                long lastUpdatedTimeStamp,
                                int entityVersion) {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDefinedEntity.class);

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

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("uname", objectName);
        map.putAll(props);
        map.put("createdTimeStamp", createdTimeStamp);
        map.put("lastUpdatedTimeStamp", lastUpdatedTimeStamp);
        map.put("entityVersion", entityVersion);
        return map;
    }

    public static class Builder {
        private final String typeName;
        private final String objectName;
        private final int entityVersion;
        private final Map<String, Object> props = new HashMap<>();

        public Builder(String typeName, String objectName, int entityVersion) {
            this.typeName = typeName;
            this.objectName = objectName;
            this.entityVersion = entityVersion;
        }

        public Builder setProps(Map<String, Object> props) {
            this.props.putAll(props);
            return this;
        }

        public UserDefinedEntity build() {
            Preconditions.checkNotNull(typeName);
            Preconditions.checkNotNull(objectName);
            return new UserDefinedEntity(typeName, objectName, props, Time.now(), Time.now(), entityVersion);
        }

    }

    // test to see if some null cases could mess this?
    // we check typename and object name when we build the UserDefinedObject, but no checks for props, timestamps or version
    public Udo toUdo() {
        return Udo.builder(typeName, objectName)
                .setProps(props != null ? props : new HashMap<>())
                .setCreateTimestamp(createdTimeStamp)
                .setLastUpdateTimestamp(lastUpdatedTimeStamp)
                .setEntityVersion(entityVersion)
                .build();
    }


    public static Udo fromRecord(Record record, Schema icebergSchema, String typeName) {
        Preconditions.checkNotNull(record);
        Preconditions.checkNotNull(icebergSchema);
        Preconditions.checkNotNull(typeName);

        String name = String.valueOf(record.getField("uname"));
        Long createdTimestamp = record.getField("createdTimeStamp") != null
                ? ((Number) record.getField("createdTimeStamp")).longValue()
                : null;
        Long lastUpdatedTimestamp = record.getField("lastUpdatedTimeStamp") != null
                ? ((Number) record.getField("lastUpdatedTimeStamp")).longValue()
                : null;
        Integer entityVersion = record.getField("entityVersion") != null
                ? ((Number) record.getField("entityVersion")).intValue()
                : null;

        // Dynamically build props from the schema
        Map<String, Object> props = new HashMap<>();
        for (Types.NestedField field : icebergSchema.columns()) {
            String fieldName = field.name();
            if (!Set.of("uname", "createdTimeStamp", "lastUpdatedTimeStamp", "entityVersion").contains(fieldName)) {
                Object value = record.getField(fieldName);
                if (value != null) {
                    props.put(fieldName, value);
                }
            }
        }

        return Udo.builder(typeName, name)
                .setProps(props)
                .setCreateTimestamp(createdTimestamp)
                .setLastUpdateTimestamp(lastUpdatedTimestamp)
                .setEntityVersion(entityVersion)
                .build();
    }


}
