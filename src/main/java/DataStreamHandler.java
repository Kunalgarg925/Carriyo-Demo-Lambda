import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DataStreamHandler  implements RequestHandler<DynamodbEvent, String> {

    private final RestHighLevelClient client;
    private static final String INDEX_NAME = "shipment-index";
    private static final String ELASTIC_SEARCH_HOST = System.getenv("ELASTIC_SEARCH_HOST");
    private static final String ELASTIC_SEARCH_HOST_AUTHORIZATION_KEY = System.getenv("ES_AUTORIZATION_API_KEY");

    public DataStreamHandler() {
        this.client = new RestHighLevelClient(
            RestClient.builder( new HttpHost(ELASTIC_SEARCH_HOST, 443, "https"))
                .setDefaultHeaders(new BasicHeader[]{new BasicHeader("Authorization", "ApiKey " + ELASTIC_SEARCH_HOST_AUTHORIZATION_KEY)}
            )
        );
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

                    // Index or update the document in Elasticsearch
                    IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
                            .id(shipmentId)
                            .source(jsonString, XContentType.JSON);
                    IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                    context.getLogger().log("Document indexed with status: " + response.status());
                } else if ("REMOVE".equals(eventName)) {
                    // Delete the document from Elasticsearch
                    DeleteRequest deleteRequest = new DeleteRequest(INDEX_NAME, shipmentId);
                    DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
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
