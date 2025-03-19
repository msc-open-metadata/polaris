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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public record UserDefinedEntityType(String typeName, Map<String, PropertyDefinition> propertyDefinitions) {
    /**
     * Constructor for creating a new user-defined entity type.
     *
     * @param typeName            The name of the entity type
     * @param propertyDefinitions The property definitions for this entity type.
     */
    public UserDefinedEntityType {
    }

    /**
     * Process a Map structure into property definitions.
     * This handles prop map structures and converts them to appropriate PropertyType values.
     *
     * @code example:
     * "props": {
     * "def": "variant"
     * "language": "string"
     * "return_type": "string"
     * }
     */
    public static Map<String, PropertyDefinition> fromMap(Map<String, String> propMap) {
        if (propMap == null) {
            return new HashMap<>();
        }

        Map<String, PropertyDefinition> result = new HashMap<>();

        for (Map.Entry<String, String> entry : propMap.entrySet()) {
            String propName = entry.getKey();
            String typeStr = entry.getValue();
            PropertyType propType = determinePropertyTypeFromString(typeStr);
            result.put(propName, new PropertyDefinition(propName, propType));
        }

        return result;
    }

    private static PropertyType determinePropertyTypeFromString(String typeStr) {
        return switch (typeStr.toLowerCase(Locale.ROOT)) {
            case "string" -> PropertyType.STRING;
            case "number" -> PropertyType.NUMBER;
            case "boolean" -> PropertyType.BOOLEAN;
            case "date" -> PropertyType.DATE;
            case "map", "object", "list" -> PropertyType.VARIANT;
            default -> PropertyType.STRING; // Default to STRING for unknown types
        };
    }

    /**
     * Property types for user-defined entity properties.
     * {@code @example:} def: VARIANT, name: STRING
     */
    public enum PropertyType {
        STRING, NUMBER, BOOLEAN, DATE, VARIANT
    }

    public static class PropertyDefinition {
        private final String propName;
        private final PropertyType propType;

        public PropertyDefinition(String propName, PropertyType propType) {
            this.propName = propName;
            this.propType = propType;
        }

    }

    /**
     * Builder for creating UserDefinedEntityType instances.
     */
    public static class Builder {
        private final Map<String, PropertyDefinition> props = new HashMap<>();
        private String typeName;
        //TODO required property?

        public Builder setTypeName(String typeName) {
            this.typeName = typeName;
            return this;
        }

        public Builder addProperty(String name, PropertyType type) {
            props.put(name, new PropertyDefinition(name, type));
            return this;
        }

        public Builder addProperties(Map<String, PropertyDefinition> properties) {
            props.putAll(properties);
            return this;
        }

        public UserDefinedEntityType build() {
            if (typeName == null || typeName.isEmpty()) {
                throw new IllegalStateException("Entity type typeName is required");
            }
            return new UserDefinedEntityType(typeName, props);
        }
    }
}
