import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;

public class Utility {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static String convertDynamoDBToJson(Map<String, AttributeValue> dynamoDBItem) throws Exception {
        ObjectNode jsonObject = objectMapper.createObjectNode();

        for (Map.Entry<String, AttributeValue> entry : dynamoDBItem.entrySet()) {
            String key = entry.getKey();
            AttributeValue value = entry.getValue();

            if (value.getS() != null) {
                jsonObject.put(key, value.getS());
            } else if (value.getN() != null) {
                jsonObject.put(key, Integer.parseInt(value.getN()));
            } else if (value.getL() != null) {
                ArrayNode arrayNode = objectMapper.createArrayNode();
                for (AttributeValue item : value.getL()) {
                    if (item.getM() != null) {
                        arrayNode.add(objectMapper.readTree(convertDynamoDBToJson(item.getM())));
                    } else if (item.getS() != null) {
                        arrayNode.add(item.getS());
                    } else if (item.getN() != null) {
                        arrayNode.add(Integer.parseInt(item.getN()));
                    }
                }
                jsonObject.set(key, arrayNode);
            } else if (value.getM() != null) {
                jsonObject.set(key, objectMapper.readTree(convertDynamoDBToJson(value.getM())));
            }
        }

        return objectMapper.writeValueAsString(jsonObject);
    }
}
