package org.apache.polaris.extension.opendic.entity;

import com.google.common.base.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.polaris.extension.opendic.model.CreatePlatformMappingRequest;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Define platform-specific mappings for User Defined Objects (UDOs).
 * Enables translating an UDO into platform-specific syntax based on its type and properties.
 * Used to return 'dump' sql for synced UDOs.
 * <p>
 * Exxample: function UDO mapped to Spark SQL syntax:
 * <p>
 * // Create a mapping for Spark SQL functions with a specific template (syntax)
 * UserDefinedPlatformMapping sparkMapping = new UserDefinedPlatformMapping(
 * "function",  // The type name this mapping applies to
 * "Spark",     // The target platform
 * "CREATE FUNCTION {name}({args}) RETURNS {return_type} LANGUAGE {language} AS '{definition}'"
 * );
 * <p>
 * // Map UDO properties to template placeholders
 * sparkMapping.mapProperty("name", "name");
 * sparkMapping.mapProperty("args", "props.args");
 * sparkMapping.mapProperty("return_type", "props.return_type");
 * sparkMapping.mapProperty("language", "props.language");
 * sparkMapping.mapProperty("definition", "props.definition");
 * <p>
 * // Generate the dump/SQL
 * String sparkSql = sparkMapping.generateSQL(myFunctionUdo);
 *
 * @param typeName       The UDO type this is a mapping for, must be the name of an existing type.
 * @param platformName   The target platform name, (e.g. Spark or Snowflake)
 * @param templateSyntax The template with placeholders (e.g., "CREATE FUNCTION {name}(...)")
 * @param objectDumpMap  A map from templateSyntax placeholder value object that defines their dump string resolution  (e.g., "args": {"type": "map", "format": "<key> <value>", "delimiter": ", "})
 */
public record UserDefinedPlatformMapping(String typeName,
                                         String platformName,
                                         String templateSyntax,
                                         Map<String, Map<String, String>> objectDumpMap,
                                         Schema icebergSchema,
                                         OffsetDateTime createdTimeStamp,
                                         OffsetDateTime lastUpdatedTimeStamp,
                                         int entityVersion) {


    public static UserDefinedPlatformMapping fromRequest(String typeName, String platformName, CreatePlatformMappingRequest request) {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(request.getPlatformMapping());
        Preconditions.checkArgument(!request.getPlatformMapping().getSyntax().isEmpty(), "Syntax cannot be empty");

        return new Builder(typeName, platformName, request.getPlatformMapping().getSyntax(), 1)
                .setIcbergSchema(UserDefinedPlatformMapping.getIcebergSchema())
                .build();
    }

    public static Schema getIcebergSchema() {
        return new Schema(
                Types.NestedField.required(1, "type", org.apache.iceberg.types.Types.StringType.get()),
                Types.NestedField.required(2, "platform", org.apache.iceberg.types.Types.StringType.get()),
                Types.NestedField.required(3, "syntax", org.apache.iceberg.types.Types.StringType.get()),
                Types.NestedField.optional(4, "object_dump_map", org.apache.iceberg.types.Types.MapType.ofOptional(5, 6,
                        org.apache.iceberg.types.Types.StringType.get(),
                        org.apache.iceberg.types.Types.StructType.of(
                                Types.NestedField.required(7, "type", org.apache.iceberg.types.Types.StringType.get()),
                                Types.NestedField.required(8, "format", org.apache.iceberg.types.Types.StringType.get()),
                                Types.NestedField.optional(9, "delimiter", org.apache.iceberg.types.Types.StringType.get())
                        )
                )),
                Types.NestedField.optional(10, "created_time", org.apache.iceberg.types.Types.TimestampType.withZone()),
                Types.NestedField.optional(11, "last_updated_time", org.apache.iceberg.types.Types.TimestampType.withZone()),
                Types.NestedField.optional(12, "entity_version", org.apache.iceberg.types.Types.IntegerType.get())
        );
    }

    /**
     * Generate the platform-specific SQL from the template and mapped properties.
     *
     * @param udo The UserDefinedEntity (UDO) to generate syntax for
     * @return The generated platform-specific representation
     */
    public String generateSQL(UserDefinedEntity udo) {
        if (!udo.typeName().equals(this.typeName)) {
            throw new IllegalArgumentException(
                    "UDO type '" + udo.typeName() + "' does not match mapping type '" +
                            this.typeName + "'"
            );
        }

        String result = templateSyntax;
        return result;
    }

    public Map<String, Object> toObjMap() {
        Map<String, Object> result = new HashMap<>();
        result.put("type", typeName);
        result.put("platform", platformName);
        result.put("syntax", templateSyntax);
        result.put("object_dump_map", objectDumpMap);
        result.put("created_time", createdTimeStamp);
        result.put("last_updated_time", lastUpdatedTimeStamp);
        result.put("entity_version", entityVersion);
        return result;
    }

    public static class Builder {
        private final String typeName;
        private final String platformName;
        private final String templateSyntax;
        private final Map<String, Map<String, String>> objectDumpMap = new HashMap<>();
        private final int entityVersion;
        private Schema icebergSchema;

        public Builder(String typeName, String platformName, String templateSyntax, int entityVersion) {
            this.typeName = typeName;
            this.platformName = platformName;
            this.templateSyntax = templateSyntax;
            this.entityVersion = entityVersion;
        }

        public Builder setObjectDumpMap(Map<String, Map<String, String>> objectDumpMap) {
            this.objectDumpMap.putAll(objectDumpMap);
            return this;
        }

        public Builder setIcbergSchema(Schema schema) {
            icebergSchema = schema;
            return this;
        }

        public UserDefinedPlatformMapping build() {
            Preconditions.checkNotNull(typeName);
            Preconditions.checkNotNull(platformName);
            Preconditions.checkNotNull(templateSyntax);
            setObjectDumpMap(objectDumpMap);
            return new UserDefinedPlatformMapping(typeName, platformName, templateSyntax, objectDumpMap, icebergSchema, OffsetDateTime.now(), OffsetDateTime.now(), entityVersion);
        }
    }

}