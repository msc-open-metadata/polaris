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
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.polaris.extension.opendic.model.DefineUdoRequest;
import org.apache.polaris.extension.opendic.model.UdoSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


/**
 * A record representing user defined types in the opendict specification.
 * A type in this case is defined by a name, e.g. Function and a prop map, e.g. a list of property definitions that are
 * that this type expects. For now, this entity can be considered a userdefined table schema
 */
public record UserDefinedEntitySchema(String typeName, Map<String, PropertyType> propertyDefinitions,
                                      OffsetDateTime createdTimeStamp,
                                      OffsetDateTime lastUpdatedTimeStamp,
                                      int entityVersion) {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDefinedEntitySchema.class);

    /**
     * Creating a new user-defined entity type.
     *
     * @param typeName            The name of the entity type
     * @param propertyDefinitions The property definitions for this entity type.
     */
    public UserDefinedEntitySchema {
    }

    public static UserDefinedEntitySchema fromRequest(DefineUdoRequest request) {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(request.getUdoType());
        Preconditions.checkArgument(!request.getProperties().isEmpty(), "Must define atleast 1 property");
        Preconditions.checkNotNull(request.getProperties());
        Preconditions.checkArgument(!request.getUdoType().isEmpty());
        return new UserDefinedEntitySchema.Builder()
                .setTypeName(request.getUdoType())
                .setProperties(propsFromMap(request.getProperties()))
                .build();
    }

    /**
     * Process a Map structure into property definitions.
     * This handles prop map structures and converts them to appropriate PropertyType values.
     *
     * @code example:
     * {map:
     * "def":VARIANT
     * "language":STRING
     * "return_type": "STRING"
     * "args": "VARIANT"
     * }
     */
    public static Map<String, PropertyType> propsFromMap(Map<String, String> propMap) {
        Map<String, PropertyType> result = new HashMap<>();
        if (propMap == null) {
            return result;
        }
        for (Map.Entry<String, String> entry : propMap.entrySet()) {
            String propName = entry.getKey();
            String typeStr = entry.getValue();
            if (typeStr != null) {
                PropertyType propType = propertyTypeFromString(typeStr);
                result.put(propName, propType);
            }
        }
        LOGGER.debug("Parsed props to schema map");
        return result;
    }

    public static UserDefinedEntitySchema fromTableSchema(org.apache.iceberg.Schema icebergSchema, String typeName, long createdTimeStamp, long lastUpdatedTimeStamp, int entityVersion) {
        return UserDefinedEntitySchema.fromTableSchema(
                AvroSchemaUtil.convert(icebergSchema, typeName),
                createdTimeStamp,
                lastUpdatedTimeStamp,
                entityVersion);
    }

    public static UserDefinedEntitySchema fromTableSchema(Schema schema, long createdMillis, long lastUpdatedMillis, int entityVersion) {
        Preconditions.checkNotNull(schema);

        Map<String, PropertyType> props = new HashMap<>();
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            PropertyType propType = propertyTypeFromString(fieldSchema.getType().getName());
            props.put(fieldName, propType);
        }
        return UserDefinedEntitySchema.builder()
                .setTypeName(schema.getName())
                .setProperties(props)
                .setCreateTimestamp(fromEpochMillis(createdMillis))
                .setLastUpdateTimestamp(fromEpochMillis(lastUpdatedMillis))
                .setEntityVersion(entityVersion)
                .build();
    }

    /**
     * Converts milliseconds since Unix epoch to OffsetDateTime
     *
     * @param epochMillis milliseconds since January 1, 1970, 00:00:00 GMT
     * @return OffsetDateTime representing the time at the system default offset
     */
    public static OffsetDateTime fromEpochMillis(long epochMillis) {
        Instant instant = Instant.ofEpochMilli(epochMillis);
        return OffsetDateTime.ofInstant(instant, ZoneId.systemDefault());
    }

    private static PropertyType propertyTypeFromString(String typeStr) {
        return switch (typeStr.toLowerCase(Locale.ROOT)) {
            case "string" -> PropertyType.STRING;
            case "double" -> PropertyType.DOUBLE;
            case "int" -> PropertyType.INT;
            case "boolean" -> PropertyType.BOOLEAN;
            case "date" -> PropertyType.DATE;
            case "array", "list" -> PropertyType.ARRAY;
            case "map", "object", "variant" -> PropertyType.VARIANT;
            default -> PropertyType.STRING; // Default to STRING for unknown types
        };
    }

    /**
     * Add a field to the schema with the appropriate Avro type
     */
    private static void addField(SchemaBuilder.FieldAssembler<Schema> fieldAssembler,
                                 String fieldName,
                                 UserDefinedEntitySchema.PropertyType propertyType) {

        LOGGER.debug("Converting property type to avro type: {} : {}", fieldName, propertyType.name());
        switch (propertyType) {
            case STRING:
                fieldAssembler
                        .name(fieldName)
                        .type().unionOf().nullType().and().stringType().endUnion()
                        .noDefault();
                break;

            case INT:
                fieldAssembler
                        .name(fieldName)
                        .type().unionOf().nullType().and().intType().endUnion()
                        .noDefault();
                break;

            case DOUBLE:
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

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Generate an Avro schema from a UserDefinedEntityType
     *
     * @return An Avro Schema representing the entity type
     */
    public Schema getAvroSchema() {
        LOGGER.debug("Getting Avro Schema for {}", typeName);
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder
                .record(typeName)
                .namespace("org.apache.polaris.extension.opendic.entity");
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();
        fieldAssembler
                .name("uname")
                .type().stringType()
                .noDefault();

        // Add each property as a field
        for (Map.Entry<String, UserDefinedEntitySchema.PropertyType> entry :
                propertyDefinitions.entrySet()) {

            String propName = entry.getKey();
            UserDefinedEntitySchema.PropertyType propType = entry.getValue();
            addField(fieldAssembler, propName, propType);
        }
        // Add default fields
        fieldAssembler.name("created_time").type().unionOf().nullType().and()
                .stringType().endUnion()
                .noDefault();
        fieldAssembler.name("last_updated_time").type().unionOf().nullType().and()
                .stringType().endUnion()
                .noDefault();
        fieldAssembler.name("entity_version").type().unionOf().nullType().and().intType().endUnion().noDefault();
        return fieldAssembler.endRecord();
    }

    public org.apache.iceberg.Schema getIcebergSchema() {
        var avroSchema = getAvroSchema();
        return AvroSchemaUtil.toIceberg(avroSchema);
    }

    public UdoSchema toUdoSchema() {
        Map<String, String> props = new HashMap<>();
        for (Map.Entry<String, PropertyType> entry : propertyDefinitions.entrySet()) {
            String propName = entry.getKey();
            PropertyType propType = entry.getValue();
            props.put(propName, propType.name());
        }
        return UdoSchema.builder()
                .setUdoType(typeName)
                .setProperties(props)
                .setCreatedTimestamp(createdTimeStamp.toString())
                .setLastUpdatedTimestamp(lastUpdatedTimeStamp.toString())
//                .setVersion(entityVersion) //TODO uncomment
                .build();
    }

    /**
     * Property types for user-defined entity properties.
     * {@code @example:} def: VARIANT, name: STRING
     */
    public enum PropertyType {
        STRING,
        DOUBLE,
        INT,
        BOOLEAN,
        DATE,
        VARIANT,
        ARRAY
    }

    /**
     * Builder for creating UserDefinedEntitySchema instances.
     */
    public static class Builder {
        private final Map<String, PropertyType> props = new HashMap<>();
        private String typeName;
        private OffsetDateTime createdTimeStamp = OffsetDateTime.now();
        private OffsetDateTime lastUpdatedTimeStamp = OffsetDateTime.now();
        private int entityVersion;

        public Builder setTypeName(String typeName) {
            this.typeName = typeName;
            return this;
        }

        public Builder addProperty(String name, PropertyType type) {
            props.put(name, type);
            return this;
        }

        public Builder setProperties(Map<String, PropertyType> properties) {
            props.putAll(properties);
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


        public UserDefinedEntitySchema build() {
            Preconditions.checkNotNull(typeName);
            Preconditions.checkNotNull(props);
            Preconditions.checkArgument(!props.isEmpty(), "Must define at least 1 property");
            return new UserDefinedEntitySchema(typeName, props, createdTimeStamp, lastUpdatedTimeStamp, entityVersion);
        }
    }
}
