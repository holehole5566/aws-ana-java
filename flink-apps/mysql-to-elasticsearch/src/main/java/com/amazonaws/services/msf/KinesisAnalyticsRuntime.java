package com.amazonaws.services.msf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class to load application properties for Flink applications.
 * Supports loading from both local JSON files (for development) and
 * Managed Service for Apache Flink runtime properties (for production).
 */
public class KinesisAnalyticsRuntime {
    
    private static final Logger LOG = LoggerFactory.getLogger(KinesisAnalyticsRuntime.class);
    private static final String DEFAULT_APPLICATION_PROPERTIES_FILE = "flink-application-properties-local.json";
    
    /**
     * Loads application properties from the appropriate source.
     * When running locally, loads from a JSON file in the resources folder.
     * When running on Managed Service for Apache Flink, loads from runtime properties.
     */
    public static Map<String, Properties> getApplicationProperties() throws IOException {
        // Try to load from runtime first (when running on Managed Service for Apache Flink)
        try {
            Map<String, Properties> runtimeProperties = 
                com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime.getApplicationProperties();
            if (runtimeProperties != null && !runtimeProperties.isEmpty()) {
                LOG.info("Loaded application properties from Managed Service for Apache Flink runtime");
                return runtimeProperties;
            }
        } catch (Exception e) {
            LOG.info("Not running on Managed Service for Apache Flink, loading properties from local file");
        }
        
        // Load from local file for development
        return loadApplicationPropertiesFromFile(DEFAULT_APPLICATION_PROPERTIES_FILE);
    }
    
    /**
     * Loads application properties from a JSON file.
     */
    private static Map<String, Properties> loadApplicationPropertiesFromFile(String fileName) throws IOException {
        LOG.info("Loading application properties from file: {}", fileName);
        
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Properties> applicationProperties = new HashMap<>();
        
        try (InputStream inputStream = KinesisAnalyticsRuntime.class.getClassLoader()
                .getResourceAsStream(fileName)) {
            
            if (inputStream == null) {
                throw new IOException("Unable to load properties file: " + fileName);
            }
            
            JsonNode propertiesNode = objectMapper.readTree(inputStream);
            
            for (JsonNode propertyGroup : propertiesNode) {
                String groupId = propertyGroup.get("PropertyGroupId").asText();
                Properties properties = new Properties();
                
                JsonNode propertyMap = propertyGroup.get("PropertyMap");
                Iterator<Map.Entry<String, JsonNode>> fields = propertyMap.fields();
                
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    properties.setProperty(field.getKey(), field.getValue().asText());
                }
                
                applicationProperties.put(groupId, properties);
                LOG.info("Loaded property group: {} with {} properties", groupId, properties.size());
            }
        }
        
        return applicationProperties;
    }
}
