/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.common.Strings;
import org.opensearch.geospatial.GeospatialParser;

/**
 * ContentBuilder is responsible for preparing Request that can be executed
 * to upload GeoJSON Features as Documents.
 */
public class ContentBuilder {
    public static final String GEOJSON_FEATURE_ID_FIELD = "id";
    public static final String ERROR_MESSAGE_DELIMITER = "\n";
    private final Client client;
    private static final Logger LOGGER = LogManager.getLogger(ContentBuilder.class);

    public ContentBuilder(Client client) {
        this.client = Objects.requireNonNull(client, "Client cannot be null");
    }

    public Optional<BulkRequestBuilder> prepare(UploadGeoJSONRequestContent content, String pipeline) {
        return prepareContentRequest(content, pipeline);
    }

    public void prepareBulk(UploadGeoJSONRequestContent content, String pipeline, StepListener<Void> listener) {
        performRequest(content, pipeline, listener);
    }

    private void performRequest(UploadGeoJSONRequestContent content, String pipeline, StepListener<Void> listener) {
        if (content.getData().isEmpty()) {
            listener.onFailure(new IllegalStateException("No valid features are available to index"));
            return;
        }

        BulkProcessor processor = prepareBulkProcessorBuilder(listener).
            setBackoffPolicy(BackoffPolicy.noBackoff()).build();
        content.getData()
            .stream()
            .map(GeospatialParser::toStringObjectMap)
            .map(GeospatialParser::getFeatures)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .flatMap(List::stream)
            .map(documentSource -> createIndexRequest(documentSource))
            .map(indexRequest -> indexRequest.index(content.getIndexName()))
            .map(indexRequest -> indexRequest.setPipeline(pipeline))
            .forEach(processor::add);
        processor.close();
    }

    // build BulkRequestBuilder, by, iterating, UploadGeoJSONRequestContent's data. This depends on
    // GeospatialParser.getFeatures to extract features from user input, create IndexRequestBuilder
    // with index name and pipeline.
    private Optional<BulkRequestBuilder> prepareContentRequest(UploadGeoJSONRequestContent content, String pipeline) {
        if (content.getData().isEmpty()) {
            return Optional.empty();
        }
        final BulkRequestBuilder builder = prepareBulkRequestBuilder();
        content.getData()
            .stream()
            .map(GeospatialParser::toStringObjectMap)
            .map(GeospatialParser::getFeatures)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .flatMap(List::stream)
            .map(documentSource -> createIndexRequestBuilder(documentSource))
            .map(indexRequestBuilder -> indexRequestBuilder.setIndex(content.getIndexName()))
            .map(indexRequestBuilder -> indexRequestBuilder.setPipeline(pipeline))
            .forEach(builder::add);
        if (builder.numberOfActions() < 1) { // check any features to upload
            return Optional.empty();
        }
        return Optional.of(builder);
    }

    private BulkProcessor.Builder prepareBulkProcessorBuilder(StepListener<Void> listener) {

//        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = (bulkRequest, bulkResponseActionListener) -> {
//            client.bulk(bulkRequest, ActionListener.wrap(bulkResponse -> {
//                if (bulkResponse.hasFailures()) {
//                    final String failureMessage = buildUploadFailureMessage(bulkResponse);
//                    throw new IllegalStateException(failureMessage);
//                }
//                LOGGER.info("indexed " + bulkResponse.getItems().length + " features");
//                //listener.onResponse(null);
//                bulkResponseActionListener.onResponse(bulkResponse);
//            }, bulkRequestFailedException -> {
//                StringBuilder message = new StringBuilder("Failed to index document due to ").append(bulkRequestFailedException.getMessage());
//                bulkResponseActionListener.onFailure(bulkRequestFailedException);
//                //listener.onFailure(new IllegalStateException(message.toString()));
//            }));
//        };

        return BulkProcessor.builder(client::bulk, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                LOGGER.info(" no of actions " + bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                listener.onResponse(null);
                LOGGER.info(bulkResponse.buildFailureMessage());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                listener.onFailure(new IllegalStateException(throwable.getMessage()));
                LOGGER.info(throwable.getMessage());
            }
        }).setConcurrentRequests(1).setBulkActions(10);

    }

    private String buildUploadFailureMessage(BulkResponse bulkResponse) {
        return Stream.of(bulkResponse.getItems())
            .filter(BulkItemResponse::isFailed)
            .map(BulkItemResponse::getFailureMessage)
            .collect(Collectors.joining(ERROR_MESSAGE_DELIMITER));
    }

    private BulkRequestBuilder prepareBulkRequestBuilder() {
        return client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
    }

    private IndexRequestBuilder createIndexRequestBuilder(Map<String, Object> source) {
        final IndexRequestBuilder requestBuilder = client.prepareIndex().setSource(source);
        String id = GeospatialParser.extractValueAsString(source, GEOJSON_FEATURE_ID_FIELD);
        return Strings.hasText(id) ? requestBuilder.setId(id) : requestBuilder;
    }

    private IndexRequest createIndexRequest(Map<String, Object> source) {
        final IndexRequest request = new IndexRequest().create(true).source(source);
        String id = GeospatialParser.extractValueAsString(source, GEOJSON_FEATURE_ID_FIELD);
        return Strings.hasText(id) ? request.id(id) : request;
    }
}
