import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import service.ElasticSearchClient;

import java.io.IOException;
import java.util.Map;

public class DataStreamHandler  implements RequestHandler<DynamodbEvent, String> {

    private final ElasticSearchClient elasticSearchClient;
    public DataStreamHandler() {
        this.elasticSearchClient = new ElasticSearchClient();
    }
    @Override
    public String handleRequest(DynamodbEvent dynamodbEvent, Context context) {

        dynamodbEvent.getRecords().forEach(record -> {
            String eventName = record.getEventName();
            try {
                String shipmentId = record.getDynamodb().getKeys().get("shipmentId").toString();
                if ("INSERT".equals(eventName) || "MODIFY".equals(eventName)) {
                    Map<String, AttributeValue> newItem = record.getDynamodb().getNewImage();
                    String jsonString = Utility.convertDynamoDBToJson(newItem);
                    IndexResponse response = elasticSearchClient.addOrUpdateDataset(shipmentId,jsonString);
                    context.getLogger().log("Document indexed with status: " + response.status());
                } else if ("REMOVE".equals(eventName)) {
                    DeleteResponse deleteResponse = elasticSearchClient.deleteDataset(shipmentId);
                    context.getLogger().log("Document deleted with ID: " + shipmentId + " status : " + deleteResponse.status());
                }
            } catch (IOException e) {
                context.getLogger().log("Error processing document: " + e.getMessage());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return "Processed " + dynamodbEvent.getRecords().size() + " records.";
    }
}
