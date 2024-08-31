package service;

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

public class ElasticSearchClient {
    private final RestHighLevelClient elasticSearchClient;
    private static final String INDEX_NAME = "shipment-index";
    private static final String ELASTIC_SEARCH_HOST = System.getenv("ELASTIC_SEARCH_HOST");
    private static final String ELASTIC_SEARCH_HOST_AUTHORIZATION_KEY = System.getenv("ES_AUTHORIZATION_API_KEY");
    public ElasticSearchClient(){
        this.elasticSearchClient = new RestHighLevelClient(
                RestClient.builder( new HttpHost(ELASTIC_SEARCH_HOST, 443, "https"))
                        .setDefaultHeaders(new BasicHeader[]{new BasicHeader("Authorization", "ApiKey " + ELASTIC_SEARCH_HOST_AUTHORIZATION_KEY)}
                        )
        );
    }

    public IndexResponse addOrUpdateDataset(String shipmentId, String jsonString) throws IOException {
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
                .id(shipmentId)
                .source(jsonString, XContentType.JSON);
        return elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);
    }
    public DeleteResponse deleteDataset(String shipmentId) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX_NAME, shipmentId);
        return elasticSearchClient.delete(deleteRequest, RequestOptions.DEFAULT);
    }
}
