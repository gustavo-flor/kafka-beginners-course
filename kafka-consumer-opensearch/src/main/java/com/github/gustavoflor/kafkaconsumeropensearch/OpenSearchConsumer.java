package com.github.gustavoflor.kafkaconsumeropensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;

import java.net.URI;

import static java.util.Objects.isNull;

public class OpenSearchConsumer {
    private static final URI OPENSEARCH_URI = URI.create("http://localhost:9200");

    public static void main(String[] args) {
        RestHighLevelClient client = client();

    }

    public static RestHighLevelClient client() {
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
}
