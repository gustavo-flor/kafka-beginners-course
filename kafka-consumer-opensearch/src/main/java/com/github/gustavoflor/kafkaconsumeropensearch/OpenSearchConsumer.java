package com.github.gustavoflor.kafkaconsumeropensearch;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static java.util.Objects.isNull;

public class OpenSearchConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchConsumer.class);
    private static final URI OPENSEARCH_URI = URI.create("http://localhost:9200");
    private static final String WIKIMEDIA_INDEX = "wikimedia";
    private static final String WIKIMEDIA_TOPIC = "wikimedia.recentchange";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = client();
        KafkaConsumer<String, String> kafkaConsumer = kafkaConsumer();
        kafkaConsumer.subscribe(Collections.singletonList(WIKIMEDIA_TOPIC));
        try (client; kafkaConsumer) {
            createWikimediaIndexIfNotExists(client);
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
                LOG.info("Received {} record(s)", records.count());
                createWikimediaDocumentsInBulk(client, records);
                kafkaConsumer.commitSync();
            }
        }
    }

    private static boolean isJson(String value) {
        try {
            JsonParser.parseString(value);
        } catch (JsonSyntaxException e) {
            return false;
        }
        return true;
    }

    private static RestHighLevelClient client() {
        String userInfo = OPENSEARCH_URI.getUserInfo();
        if (isNull(userInfo)) {
            HttpHost httpHost = new HttpHost(OPENSEARCH_URI.getHost(), OPENSEARCH_URI.getPort(), "http");
            return new RestHighLevelClient(RestClient.builder(httpHost));
        }
        String[] auth = userInfo.split(":");
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
        HttpHost httpHost = new HttpHost(OPENSEARCH_URI.getHost(), OPENSEARCH_URI.getPort(), OPENSEARCH_URI.getScheme());
        RestClientBuilder clientBuilder = RestClient.builder(httpHost).setHttpClientConfigCallback(httpClientConfigCallback(credentialsProvider));
        return new RestHighLevelClient(clientBuilder);
    }

    private static RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback(CredentialsProvider credentialsProvider) {
        DefaultConnectionKeepAliveStrategy connectionKeepAliveStrategy = new DefaultConnectionKeepAliveStrategy();
        return clientBuilder -> clientBuilder.setDefaultCredentialsProvider(credentialsProvider).setKeepAliveStrategy(connectionKeepAliveStrategy);
    }

    private static KafkaConsumer<String, String> kafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "open-search-consumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(properties);
    }

    private static void createWikimediaIndexIfNotExists(RestHighLevelClient client) throws IOException {
        boolean existsIndex = client.indices().exists(new GetIndexRequest(WIKIMEDIA_INDEX), RequestOptions.DEFAULT);
        if (!existsIndex) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(WIKIMEDIA_INDEX);
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            LOG.info("The {} index has been created!", WIKIMEDIA_INDEX);
        }
    }

    private static void extractDataToBulkRequest(ConsumerRecord<String, String> record, BulkRequest bulkRequest) {
        if (!isJson(record.value())) {
            return;
        }
        try {
            IndexRequest request = new IndexRequest(WIKIMEDIA_INDEX)
                    .source(record.value(), XContentType.JSON)
                    .id(getId(record));
            bulkRequest.add(request);
        } catch (Exception e) {
            LOG.error("Ops... error on extracting document to bulk request", e);
        }
    }

    private static String getId(ConsumerRecord<String, String> record) {
        return JsonParser.parseString(record.value())
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static void createWikimediaDocumentsInBulk(RestHighLevelClient client, ConsumerRecords<String, String> records) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (ConsumerRecord<String, String> record : records) {
            extractDataToBulkRequest(record, bulkRequest);
        }
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            LOG.info("Created {} documents on OpenSearch", bulkResponse.getItems().length);
        }
    }
}
