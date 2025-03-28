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
import org.apache.polaris.extension.opendic.model.CreateUdoRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

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
}
