package org.apache.polaris.extension.opendic.entity;

import com.google.common.base.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.polaris.extension.opendic.model.CreatePlatformMappingRequest;
import org.apache.polaris.extension.opendic.model.PlatformMapping;
import org.apache.polaris.extension.opendic.model.PlatformMappingObjectDumpMapValue;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
 * @param typeName                 The UDO type this is a mapping for, must be the name of an existing type.
 * @param platformName             The target platform name, (e.g. Spark or Snowflake)
 * @param templateSyntax           The template with placeholders (e.g., "CREATE FUNCTION {name}(...)")
 * @param additionalSyntaxPropsMap A map from templateSyntax placeholder value object that defines their dump string resolution  (e.g., "args": {"type": "map", "format": "<key> <value>", "delimiter": ", "})
 */
public record UserDefinedPlatformMapping(String typeName,
                                         String platformName,
                                         String templateSyntax,
                                         Map<String, AdditionalSyntaxProps> additionalSyntaxPropsMap,
                                         Schema icebergSchema,
                                         OffsetDateTime createdTimeStamp,
                                         OffsetDateTime lastUpdatedTimeStamp,
                                         int entityVersion) {

    public static final String ID_COLUMN = "uname";
    private static final Set<String> SUPPORTED_DUMP_TYPES = Set.of("map", "list");

    public static UserDefinedPlatformMapping fromRequest(String typeName, String platformName, CreatePlatformMappingRequest request) {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(request.getPlatformMapping());
        Preconditions.checkArgument(!request.getPlatformMapping().getSyntax().isEmpty(), "Syntax cannot be empty");

        Map<String, AdditionalSyntaxProps> additionalSyntaxPropsMap = additionalSyntaxPropsMapFromRequest(request.getPlatformMapping().getObjectDumpMap());


        return builder(typeName, platformName, request.getPlatformMapping().getSyntax(), 1)
                .setIcebergSchema(UserDefinedPlatformMapping.getIcebergSchema())
                .setAdditionalSyntaxPropMap(additionalSyntaxPropsMap)
                .build();
    }

    public static UserDefinedPlatformMapping fromRecord(Record record) {
        Preconditions.checkNotNull(record);

        String typeName = String.valueOf(record.getField(ID_COLUMN));
        String platformName = String.valueOf(record.getField("platform"));
        String templateSyntax = String.valueOf(record.getField("syntax"));
        OffsetDateTime createdTimestamp = OffsetDateTime.parse(String.valueOf(record.getField("created_time")));
        OffsetDateTime lastUpdatedTimestamp = OffsetDateTime.parse(String.valueOf(record.getField("last_updated_time")));
        int entityVersion = (int) record.getField("entity_version");

        // Validate additionalSyntax properties from the record
        Map<String, AdditionalSyntaxProps> additionalSyntaxPropsMap = new HashMap<>();
        Object objectAdditionalSyntaxPropsField = record.getField("object_dump_map");
        Preconditions.checkNotNull(objectAdditionalSyntaxPropsField);
        Preconditions.checkArgument(objectAdditionalSyntaxPropsField instanceof Map<?, ?> additionAlSyntaxPropsMap, "object_dump_map must be a Map");
        Preconditions.checkArgument(((Map<?, ?>) objectAdditionalSyntaxPropsField).entrySet()
                .stream()
                .allMatch(entry -> entry.getKey() instanceof String && entry.getValue() instanceof Record), "object_dump_map must be a Map<String, Record>");

        // additionalSyntaxPropsMap is validated. Read the values
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) objectAdditionalSyntaxPropsField).entrySet()) {
            String key = (String) entry.getKey();
            Record value = (Record) entry.getValue();
            additionalSyntaxPropsMap.put(key, AdditionalSyntaxProps.builder()
                    .setPropType(String.valueOf(value.getField("propType")))
                    .setFormat(String.valueOf(value.getField("format")))
                    .setDelimiter(String.valueOf(value.getField("delimiter")))
                    .build());
        }

        return UserDefinedPlatformMapping.builder(typeName, platformName, templateSyntax, entityVersion)
                .setIcebergSchema(record.struct().asSchema())
                .setAdditionalSyntaxPropMap(additionalSyntaxPropsMap)
                .setCreateTimestamp(createdTimestamp)
                .setLastUpdateTimestamp(lastUpdatedTimestamp)
                .build();
    }

    private static Map<String, AdditionalSyntaxProps> additionalSyntaxPropsMapFromRequest(Map<String, PlatformMappingObjectDumpMapValue> objDumpMap) {
        Map<String, AdditionalSyntaxProps> additionalPropsMap = new HashMap<>();

        for (Map.Entry<String, PlatformMappingObjectDumpMapValue> entry : objDumpMap.entrySet()) {
            String key = entry.getKey();
            PlatformMappingObjectDumpMapValue value = entry.getValue();

            Preconditions.checkNotNull(value);
            Preconditions.checkNotNull(value.getPropType());
            Preconditions.checkNotNull(value.getFormat());
            Preconditions.checkNotNull(value.getDelimiter());
            Preconditions.checkArgument(SUPPORTED_DUMP_TYPES.contains(value.getPropType()), "Unsupported dump syntax type: {}. Supported types: {}  (%s, %s)", value.getPropType(), SUPPORTED_DUMP_TYPES);

            AdditionalSyntaxProps mapValue = AdditionalSyntaxProps.builder()
                    .setPropType(value.getPropType())
                    .setFormat(value.getFormat())
                    .setDelimiter(value.getDelimiter())
                    .build();
            additionalPropsMap.put(key, mapValue);
        }
        return additionalPropsMap;
    }

    public static Schema getIcebergSchema() {
        return new Schema(
                Types.NestedField.required(1, ID_COLUMN, Types.StringType.get()),
                Types.NestedField.required(2, "platform", Types.StringType.get()),
                Types.NestedField.required(3, "syntax", Types.StringType.get()),
                Types.NestedField.optional(4, "object_dump_map", Types.MapType.ofOptional(5, 6,
                        Types.StringType.get(),
                        AdditionalSyntaxProps.schema()
                )),
                Types.NestedField.optional(10, "created_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(11, "last_updated_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(12, "entity_version", Types.IntegerType.get())
        );
    }

    public static Builder builder(String typeName, String platformName, String templateSyntax, int entityVersion) {
        return new Builder().setTypeName(typeName)
                .setPlatformName(platformName)
                .setTemplateSyntax(templateSyntax)
                .setEntityVersion(entityVersion);
    }
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Helper to convert map data to GenericRecord based on schema
     * Reference: <<a href="https://www.tabular.io/blog/java-api-part-3/">Reference</a>>
     */
    public GenericRecord toGenericRecord() {
        var genericRecord = GenericRecord.create(icebergSchema);
        genericRecord.setField(ID_COLUMN, typeName);
        genericRecord.setField("platform", platformName);
        genericRecord.setField("syntax", templateSyntax);
        genericRecord.setField("object_dump_map", additionalSyntaxPropsMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toRecord())));
        genericRecord.setField("created_time", createdTimeStamp);
        genericRecord.setField("last_updated_time", lastUpdatedTimeStamp);
        genericRecord.setField("entity_version", entityVersion);
        return genericRecord;
    }

    public PlatformMapping toPlatformMapping() {
        var platformMapping = PlatformMapping.builder();
        platformMapping.setTypeName(typeName);
        platformMapping.setPlatformName(platformName);
        platformMapping.setSyntax(templateSyntax);
        platformMapping.setObjectDumpMap(additionalSyntaxPropsMap.entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), entry.getValue().toPlatformMappingObjectDumpMapValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        platformMapping.setCreatedTimestamp(createdTimeStamp.toString());
        platformMapping.setLastUpdatedTimestamp(lastUpdatedTimeStamp.toString());
        platformMapping.setVersion(entityVersion);
        return platformMapping.build();
    }

    /**
     * Get a list tuples container a substring of the syntax followed by a replacement value to allow for efficient dump generation and value replacement.
     */
    public List<SyntaxTuple> getSyntaxTupleList() {
        // Use a list because we want to preserve insertion order and allow for duplicates.
        List<SyntaxTuple> syntaxList = new ArrayList<>();

        // Match any word with the pattern: <word>
        final String PLACEHOLDER_REGEX = "<([^>]+)>";
        final Pattern PLACEHOLDER_PATTERN = Pattern.compile(PLACEHOLDER_REGEX);
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(templateSyntax);

        int lastMatchIdx = 0;
        while (matcher.find()) {
            String placeholder = matcher.group(1); // word
            String prefix = templateSyntax.substring(lastMatchIdx, matcher.start());
            syntaxList.add(new SyntaxTuple(prefix, placeholder));
            lastMatchIdx = matcher.end();
        }

        // Collect suffix
        String suffix = templateSyntax.substring(lastMatchIdx);
        syntaxList.add(new SyntaxTuple(suffix, ""));

        return syntaxList;
    }

    public static final class Builder {
        private String typeName;
        private String platformName;
        private String templateSyntax;
        private final Map<String, AdditionalSyntaxProps> objectDumpMap = new HashMap<>();
        private int entityVersion;
        private Schema icebergSchema;
        private OffsetDateTime createdTimeStamp = OffsetDateTime.now();
        private OffsetDateTime lastUpdatedTimeStamp = OffsetDateTime.now();

        public Builder setTypeName(String typeName) {
            this.typeName = typeName;
            return this;
        }

        public Builder setPlatformName(String platformName) {
            this.platformName = platformName;
            return this;
        }

        public Builder setTemplateSyntax(String templateSyntax) {
            this.templateSyntax = templateSyntax;
            return this;
        }

        public Builder setEntityVersion(int entityVersion) {
            this.entityVersion = entityVersion;
            return this;
        }

        public Builder setAdditionalSyntaxPropMap(Map<String, AdditionalSyntaxProps> additionalSyntaxpropMap) {
            this.objectDumpMap.putAll(additionalSyntaxpropMap);
            return this;
        }

        public Builder setIcebergSchema(Schema schema) {
            icebergSchema = schema;
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

        public UserDefinedPlatformMapping build() {
            Preconditions.checkNotNull(typeName);
            Preconditions.checkNotNull(platformName);
            Preconditions.checkNotNull(templateSyntax);
            return new UserDefinedPlatformMapping(typeName, platformName, templateSyntax, objectDumpMap, icebergSchema, createdTimeStamp, lastUpdatedTimeStamp, entityVersion);
        }
    }

    /**
     * @param propType The type of the complex object. Examples: list, map
     * @param format How to unfold the complex object. Examples: <key> <value>, <package>
     * @param delimiter The delimiter used to separate the values in the complex object. Examples: ", ", " "
     */
    public record AdditionalSyntaxProps(
            String propType,
            String format,
            String delimiter
    ) {
        public static Builder builder() {
            return new Builder();
        }

        public static Types.StructType schema() {
            return Types.StructType.of(
                    Types.NestedField.required(7, "propType", Types.StringType.get()),
                    Types.NestedField.required(8, "format", Types.StringType.get()),
                    Types.NestedField.required(9, "delimiter", Types.StringType.get()));
        }

        public Record toRecord() {
            Record struct = GenericRecord.create(schema());
            struct.setField("propType", propType);
            struct.setField("format", format);
            struct.setField("delimiter", delimiter);
            return struct;
        }

        public PlatformMappingObjectDumpMapValue toPlatformMappingObjectDumpMapValue() {
            return PlatformMappingObjectDumpMapValue.builder()
                    .setPropType(propType)
                    .setFormat(format)
                    .setDelimiter(delimiter)
                    .build();
        }

        public static class Builder {
            private String propType;
            private String format;
            private String delimiter;

            public Builder setPropType(String propType) {
                this.propType = propType;
                return this;
            }

            public Builder setFormat(String format) {
                this.format = format;
                return this;
            }

            public Builder setDelimiter(String delimiter) {
                this.delimiter = delimiter;
                return this;
            }

            public AdditionalSyntaxProps build() {
                Preconditions.checkNotNull(propType);
                Preconditions.checkNotNull(format);
                Preconditions.checkNotNull(delimiter);
                return new AdditionalSyntaxProps(propType, format, delimiter);
            }
        }
    }

    /**
     * @param prefix The substring between the previous placeholder and the current placeholder.
     * @param placeholder The value to be replaced by the object value
     */
    public record SyntaxTuple(String prefix, String placeholder) {
        /**
         * @param prefix The substring between the previous placeholder and the current placeholder.
         * @param placeholder The value to be replaced by the object value
         */
        public SyntaxTuple(String prefix, String placeholder) {
            Preconditions.checkNotNull(prefix);
            Preconditions.checkNotNull(placeholder);
            this.prefix = prefix;
            this.placeholder = placeholder;
        }

        @Override
        public String toString() {
            return prefix + placeholder;
        }
    }
}