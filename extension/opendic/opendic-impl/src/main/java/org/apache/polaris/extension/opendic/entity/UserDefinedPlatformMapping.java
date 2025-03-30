package org.apache.polaris.extension.opendic.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * Define platform-specific mappings for User Defined Objects (UDOs).
 * Enables translating an UDO into platform-specific syntax based on its type and properties.
 * Used to return 'dump' sql for synced UDOs.
 * 
 * Exxample: function UDO mapped to Spark SQL syntax:
 *
 * // Create a mapping for Spark SQL functions with a specific template (syntax)
 * UserDefinedPlatformMapping sparkMapping = new UserDefinedPlatformMapping(
 *     "function",  // The type name this mapping applies to
 *     "Spark",     // The target platform
 *     "CREATE FUNCTION {name}({args}) RETURNS {return_type} LANGUAGE {language} AS '{definition}'"
 * );
 * 
 * // Map UDO properties to template placeholders
 * sparkMapping.mapProperty("name", "name");           
 * sparkMapping.mapProperty("args", "props.args");     
 * sparkMapping.mapProperty("return_type", "props.return_type");
 * sparkMapping.mapProperty("language", "props.language");
 * sparkMapping.mapProperty("definition", "props.definition");
 * 
 * // Generate the dump/SQL
 * String sparkSql = sparkMapping.generateSQL(myFunctionUdo);
 *
 */
public class UserDefinedPlatformMapping {
    private final String typeName;
    private final String platformName;
    private final String templateSyntax;
    private final Map<String, String> propertyMappings;

    /**
     * Creates a new platform mapping.
     *
     * @param typeName The UDO type  this is a mapping for
     * @param platformName The target platform name, (e.g. Spark or Snowflake)
     * @param templateSyntax The template with placeholders (e.g., "CREATE FUNCTION {name}(...)")
     */
    public UserDefinedPlatformMapping(String typeName, String platformName, String templateSyntax) {
        this.typeName = typeName;
        this.platformName = platformName;
        this.templateSyntax = templateSyntax;
        this.propertyMappings = new HashMap<>();
    }

    /**
     * Maps a template placeholder to an UDO property "path" - the property name of an UDO.
     * Property paths can reference:
     * - "typeName" for the UDO's type
     * - "objectName" for the UDO's name
     * - TODO: should we add the timestamps etc.?
     * - "props.xxx" for properties in the UDO's props map
     *
     * @param placeholder The placeholder in the template (without braces)
     * @param propertyPath The property path to use for this placeholder
     */
    public void mapProperty(String placeholder, String propertyPath) {
        propertyMappings.put(placeholder, propertyPath);
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

        for (Map.Entry<String, String> entry : propertyMappings.entrySet()) {
            String placeholder = entry.getKey();
            String propertyPath = entry.getValue();
            
            // Get property value from UDO based on property "path" / the UDO property name
            String value = resolvePropertyValue(udo, propertyPath);
            
            // Replace placeholder in template
            result = result.replace("{" + placeholder + "}", value);
        }
        
        return result;
    }

    /**
     * Helper to get a property value from a UDO based on a property path - the UDO prop name.
     */
    private String resolvePropertyValue(UserDefinedEntity udo, String propertyPath) {
        if (propertyPath.equals("name")) {
            return udo.objectName();
        } else if (propertyPath.equals("typeName")) {
            return udo.typeName();
        } else if (propertyPath.startsWith("props.")) {
            String propName = propertyPath.substring(6); // Remove "props." prefix
            Object propValue = udo.props().get(propName);
            return propValue != null ? propValue.toString() : ""; 
        } else {
            // Just default to empty string for now
            return "";
        }
    }

    // Getters
    public String getTypeName() {
        return typeName;
    }

    public String getPlatformName() {
        return platformName;
    }

    public String getTemplateSyntax() {
        return templateSyntax;
    }

    public Map<String, String> getPropertyMappings() {
        return new HashMap<>(propertyMappings);
    }
}