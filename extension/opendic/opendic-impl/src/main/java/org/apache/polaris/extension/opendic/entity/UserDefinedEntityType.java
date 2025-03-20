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

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


/**
 * A record representing user defined types in the opendict specification.
 * A type in this case is defined by a name, e.g. Function and a prop map, e.g. a list of property definitions that are
 * that this type expects. For now, this entity can be considered a userdefined table schema
 */
public record UserDefinedEntityType(String typeName, Map<String, PropertyType> propertyDefinitions) {
    /**
     * Creating a new user-defined entity type.
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
     * "def": {"variant", "required"}
     * "language": "string"
     * "return_type": "string"
     * }
     * @code example:
     * {map:
     * "def":VARIANT
     * "language":STRING
     * "return_type": "STRING"
     * }
     */
    public static Map<String, PropertyType> propsFromMap(Map<String, String> propMap) {
        if (propMap == null) {
            return new HashMap<>();
        }

        Map<String, PropertyType> result = new HashMap<>();

        for (Map.Entry<String, String> entry : propMap.entrySet()) {
            String propName = entry.getKey();
            String typeStr = entry.getValue();
            PropertyType propType = determinePropertyTypeFromString(typeStr);
            result.put(propName, propType);
        }

        return result;
    }

    private static PropertyType determinePropertyTypeFromString(String typeStr) {
        return switch (typeStr.toLowerCase(Locale.ROOT)) {
            case "string" -> PropertyType.STRING;
            case "number" -> PropertyType.NUMBER;
            case "boolean" -> PropertyType.BOOLEAN;
            case "date" -> PropertyType.DATE;
            case "array" -> PropertyType.ARRAY;
            case "map", "object", "variant" -> PropertyType.VARIANT;
            default -> PropertyType.STRING; // Default to STRING for unknown types
        };
    }

    /**
     * Generate an Avro schema from a UserDefinedEntityType
     *
     * @param entityType The user-defined entity type
     * @return An Avro Schema representing the entity type
     */
    public static Schema generateSchema(UserDefinedEntityType entityType) {
        if (entityType == null) {
            throw new IllegalArgumentException("Entity type cannot be null");
        }

        // Start building a record schema with the entity type name
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder
                .record(entityType.typeName())
                //TODO system namespace??
                .namespace("org.apache.polaris.extension.opendic.entity");

        // Create fields builder
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();

        // Add each property as a field
        for (Map.Entry<String, UserDefinedEntityType.PropertyType> entry :
                entityType.propertyDefinitions().entrySet()) {

            String propName = entry.getKey();
            UserDefinedEntityType.PropertyType propType = entry.getValue();

            // Add the field with appropriate type
            addField(fieldAssembler, propName, propType);
        }
        // Add additional fields
        fieldAssembler
                .name("catalogId")
                .type().unionOf().nullType().and().longType().endUnion()
                .noDefault();
        fieldAssembler
                .name("createTimestamp")
                .type().unionOf().nullType().and().longType().endUnion()
                .noDefault();
        fieldAssembler
                .name("lastUpdateTimestamp")
                .type().unionOf().nullType().and().longType().endUnion()
                .noDefault();
        fieldAssembler
                .name("entityVersion")
                .type().unionOf().nullType().and().intType().endUnion()
                .noDefault();

        return fieldAssembler.endRecord();
    }

    /**
     * Add a field to the schema with the appropriate Avro type
     */
    private static void addField(SchemaBuilder.FieldAssembler<Schema> fieldAssembler,
                                 String fieldName,
                                 UserDefinedEntityType.PropertyType propertyType) {

        switch (propertyType) {
            case STRING:
                fieldAssembler
                        .name(fieldName)
                        .type().unionOf().nullType().and().stringType().endUnion()
                        .noDefault();
                break;

            case NUMBER:
                fieldAssembler
                        .name(fieldName)
                        .type().unionOf().nullType().and().doubleType().endUnion()
                        .noDefault();
                break;

            case BOOLEAN:
                fieldAssembler
                        .name(fieldName)
                        .type().unionOf().nullType().and().booleanType().endUnion()
                        .noDefault();
                break;

            case DATE:
                Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
                fieldAssembler
                        .name(fieldName)
                        .type().unionOf().nullType().and().type(dateSchema).endUnion()
                        .noDefault();
                break;
            case ARRAY:
                fieldAssembler
                        .name(fieldName)
                        .type().unionOf().nullType().and()
                        .array().items().stringType()
                        .endUnion()
                        .noDefault();
                break;
            case VARIANT:
                fieldAssembler
                        .name(fieldName)
                        .type().unionOf()
                        .nullType().and()
                        .stringType().and()
                        .doubleType().and()
                        .booleanType().and()
                        .map().values().stringType()
                        .endUnion()
                        .noDefault();
                break;

            default:
                // Default to string for unknown types
                fieldAssembler
                        .name(fieldName)
                        .type().unionOf().nullType().and().stringType().endUnion()
                        .noDefault();
        }
    }

    /**
     * Property types for user-defined entity properties.
     * {@code @example:} def: VARIANT, name: STRING
     */
    public enum PropertyType {
        STRING, STRING_OPTIONAL,
        NUMBER, NUMBER_OPTIONAL,
        BOOLEAN, BOOLEAN_OPTIONAL,
        DATE, DATE_OPTIONAL,
        VARIANT, VARIANT_OPTIONAL,
        ARRAY, ARRAY_OPTIONAL
    }

    /**
     * Builder for creating UserDefinedEntityType instances.
     */
    public static class Builder {
        private final Map<String, PropertyType> props = new HashMap<>();
        private final String typeName;
        //TODO required property?

        public Builder(String typeName) {
            this.typeName = typeName;
        }

        public Builder addProperty(String name, PropertyType type) {
            props.put(name, type);
            return this;
        }

        public Builder setProperties(Map<String, PropertyType> properties) {
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
