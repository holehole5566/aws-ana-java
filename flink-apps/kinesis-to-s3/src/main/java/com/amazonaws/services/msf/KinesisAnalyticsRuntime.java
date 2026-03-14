package com.amazonaws.services.msf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KinesisAnalyticsRuntime {
    
    private static final ObjectMapper mapper = new ObjectMapper();

    public static Map<String, Properties> getApplicationProperties() throws IOException {
        String env = System.getProperty("environment", "local");
        String filename = "flink-application-properties-" + env + ".json";
        
        InputStream input = KinesisAnalyticsRuntime.class.getClassLoader().getResourceAsStream(filename);
        if (input == null) {
            throw new IOException("Config file not found: " + filename);
        }

        JsonNode root = mapper.readTree(input);
        Map<String, Properties> result = new HashMap<>();

        for (JsonNode group : root) {
            String groupId = group.get("PropertyGroupId").asText();
            Properties props = new Properties();
            
            JsonNode propertyMap = group.get("PropertyMap");
            propertyMap.fields().forEachRemaining(entry -> 
                props.setProperty(entry.getKey(), entry.getValue().asText())
            );
            
            result.put(groupId, props);
        }

        return result;
    }
}
