package org.apache.polaris.extension.opendic.entity;

import com.google.common.base.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.polaris.extension.opendic.model.CreatePlatformMappingRequest;
import org.apache.polaris.extension.opendic.model.PlatformMappingObjectDumpMapValue;

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

        Map<String, Map<String, String>> objectDumpMap = getObjectAsMapOfMap(request);


        return builder(typeName, platformName, request.getPlatformMapping().getSyntax(), 1)
                .setIcebergSchema(UserDefinedPlatformMapping.getIcebergSchema())
                .setObjectDumpMap(objectDumpMap)
                .build();
    }

    private static Map<String, Map<String, String>> getObjectAsMapOfMap(CreatePlatformMappingRequest request) {
        Map<String, Map<String, String>> objectDumpMap = new HashMap<>();

        for (Map.Entry<String, PlatformMappingObjectDumpMapValue> entry : request.getPlatformMapping().getObjectDumpMap().entrySet()) {
            String key = entry.getKey();
            PlatformMappingObjectDumpMapValue value = entry.getValue();
            Map<String, String> mapValue = new HashMap<>();
            mapValue.put("propType", value.getPropType());
            mapValue.put("format", value.getFormat());
            mapValue.put("delimiter", value.getDelimiter());
            objectDumpMap.put(key, mapValue);
        }
        return objectDumpMap;
    }

    public static Schema getIcebergSchema() {
        var dumpStructSchema = Types.MapType.ofRequired(7, 8,
                Types.StringType.get(),
                Types.StringType.get()
        );
        return new Schema(
                Types.NestedField.required(1, "uname", Types.StringType.get()),
                Types.NestedField.required(2, "platform", Types.StringType.get()),
                Types.NestedField.required(3, "syntax", Types.StringType.get()),
                Types.NestedField.optional(4, "object_dump_map", Types.MapType.ofOptional(5, 6,
                        Types.StringType.get(),
                        dumpStructSchema
                )),
                Types.NestedField.optional(10, "created_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(11, "last_updated_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(12, "entity_version", Types.IntegerType.get())
        );
    }

    public static Builder builder(String typeName, String platformName, String templateSyntax, int entityVersion) {
        return new Builder(typeName, platformName, templateSyntax, entityVersion);
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
        result.put("uname", typeName);
        result.put("platform", platformName);
        result.put("syntax", templateSyntax);
        result.put("object_dump_map", objectDumpMap);
        result.put("created_time", createdTimeStamp);
        result.put("last_updated_time", lastUpdatedTimeStamp);
        result.put("entity_version", entityVersion);
        return result;
    }

    /**
     * Helper to convert map data to GenericRecord based on schema
     * Reference: <<a href="https://www.tabular.io/blog/java-api-part-3/">Reference</a>>
     */
    public GenericRecord toGenericRecord() {
        var genericRecord = GenericRecord.create(icebergSchema);
        return genericRecord.copy(toObjMap());
    }

    public static final class Builder {
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

        public Builder setIcebergSchema(Schema schema) {
            icebergSchema = schema;
            return this;
        }

        public UserDefinedPlatformMapping build() {
            Preconditions.checkNotNull(typeName);
            Preconditions.checkNotNull(platformName);
            Preconditions.checkNotNull(templateSyntax);
            return new UserDefinedPlatformMapping(typeName, platformName, templateSyntax, objectDumpMap, icebergSchema, OffsetDateTime.now(), OffsetDateTime.now(), entityVersion);
        }
    }

}