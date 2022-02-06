/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.geospatial.action.upload.geojson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.common.Strings;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.GeospatialParser;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.ingest.Pipeline;

/**
 * Uploader will upload GeoJSON objects from UploadGeoJSONRequestContent as
 * Documents to given index. It also supports index creation if, given index doesn't exist.
 * Also, for index creation, mapping is mandatory, because, geospatial types like geo_point and
 * geo_shape cannot be inferred from the document. The corresponding field has to be defined
 * as mapping. Also, Uploader depends on {@link FeatureProcessor} to transform GeoJSON to Document,
 * by building pipeline with {@link FeatureProcessor} and add it to IndexRequestBuilder.
 * It also uses BulkRequestBuilder to perform index operation.
 */
public class Uploader {

    public static final String FIELD_TYPE_KEY = "type";
    public static final String MAPPING_PROPERTIES_KEY = "properties";
    public static final String MAPPING_TYPE = "_doc";

    private final Logger logger = LogManager.getLogger(Uploader.class);

    private final Client client;
    private final UploadGeoJSONRequestContent content;
    private final ActionListener<AcknowledgedResponse> listener;
    private final String pipelineId;
    private final boolean shouldCreateIndex;

    public Uploader(
        Client client,
        UploadGeoJSONRequestContent content,
        boolean shouldCreateIndex,
        ActionListener<AcknowledgedResponse> listener
    ) {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.content = Objects.requireNonNull(content, "content cannot be null");
        this.listener = Objects.requireNonNull(listener, "listener cannot null");
        this.pipelineId = UUIDs.randomBase64UUID();
        this.shouldCreateIndex = shouldCreateIndex;
    }

    /**
     * upload GeoJSON Object from {@link UploadGeoJSONRequestContent} into an index, by
     * 1. Create index if POST Method
     * 2. Build Pipeline with {@link FeatureProcessor} to enrich data
     * 3. Convert all Feature in {@link UploadGeoJSONRequestContent#getData()} to {@link IndexRequestBuilder#setSource(Object...)}
     * 4. Use {@link BulkRequestBuilder} to add all {@link IndexRequestBuilder} and execute
     * 5. Delete previously created pipeline
     * @throws IOException if any of steps to create
     */
    public void upload() throws IOException {

        StepListener<Boolean> step1 = new StepListener<>();
        if (shouldCreateIndex) {
            createIndex(step1);
        }
        StepListener<Boolean> step2 = new StepListener<>();
        step1.whenComplete(aBoolean -> {
            createPipeline(step2);
        }, listener::onFailure);

        try{
            step2.whenComplete(aBoolean -> {
                indexGeoJSONAsDocument();
            }, listener::onFailure);

        } finally { // pipeline is created by uploader in the previous step, so delete it at all cost.
            deletePipeline();
        }
    }

    private void deletePipeline() {
        DeletePipelineRequest pipelineRequest = new DeletePipelineRequest(pipelineId);
        StringBuilder message = new StringBuilder("Deleted pipeline: ").append(pipelineId);
        //client.admin().cluster().deletePipeline(pipelineRequest, listenOnlyOnFailure(message, createPipeline));
    }

    private ActionListener<AcknowledgedResponse> listenOnlyOnFailure(StringBuilder action, StepListener<Boolean> stepListener) {
        assert action != null;
        return ActionListener.wrap(acknowledgedResponse -> {
            logger.info("Success: "+action.toString());
            stepListener.onResponse(true);
        }, stepListener::onFailure);
    }

    private XContentBuilder buildPipelineBodyWithFeatureProcessor() throws IOException {
        /*
        {
              "description" : "Ingest GeoJSON into index",
              "processors" : [
                {
                  "geojson-feature" : {
                    "field" : "geospatial_field_name"
                  }
                }
              ]
            }
         */
        return XContentFactory.jsonBuilder()
            .startObject()
            .startArray(Pipeline.PROCESSORS_KEY)
            .startObject()
            .startObject(FeatureProcessor.TYPE)
            .field(FeatureProcessor.FIELD_KEY, content.getFieldName())
            .endObject()
            .endObject()
            .endArray()
            .endObject();
    }

    private void createPipeline(StepListener<Boolean> stepListener) throws IOException {
        XContentBuilder pipelineRequestXContent = buildPipelineBodyWithFeatureProcessor();
        BytesReference pipelineRequestBodyBytes = BytesReference.bytes(pipelineRequestXContent);
        PutPipelineRequest pipelineRequest = new PutPipelineRequest(pipelineId, pipelineRequestBodyBytes, XContentType.JSON);
        StringBuilder pipelineMessage = new StringBuilder("Created pipeline: ").append(pipelineId);
        client.admin().cluster().putPipeline(pipelineRequest, listenOnlyOnFailure(pipelineMessage, stepListener));
    }

    private void createIndex(StepListener<Boolean> createIndex) throws IOException {
        if(!shouldCreateIndex){
            createIndex.onResponse(true);
            return;
        }
        XContentBuilder mapping = buildMappingForIndex();
        StringBuilder message = new StringBuilder("Created index: ").append(content.getIndexName());
        CreateIndexRequest request = new CreateIndexRequest(content.getIndexName()).mapping(MAPPING_TYPE, mapping);
        client.admin()
            .indices()
            .create(request, ActionListener.wrap(createIndexResponse -> {
                logger.info("Success: "+message.toString());
                createIndex.onResponse(true);
            }, createIndex::onFailure));
    }

    private XContentBuilder buildMappingForIndex() throws IOException {
        /**
         {
            "properties": {
                "field_name": {
                    "type": "geospatial_field_type"
                }
            }
         }
         */
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject(MAPPING_PROPERTIES_KEY)
            .startObject(content.getFieldName())
            .field(FIELD_TYPE_KEY, content.getFieldType())
            .endObject()
            .endObject()
            .endObject();
    }

    private BulkRequestBuilder buildBulkRequestBuilder() {
        BulkRequestBuilder builder = createBulkRequest();
        List<Object> documents = (List<Object>) content.getData();
        documents.stream()
            .map(GeospatialParser::toStringObjectMap)
            .map(document -> createIndexRequestBuilder(document))
            .forEach(builder::add);
        return builder;
    }

    private void indexGeoJSONAsDocument() {
            BulkRequestBuilder bulkRequestBuilder = buildBulkRequestBuilder();
            bulkRequestBuilder.execute(ActionListener.wrap(bulkResponse -> {
                if (!bulkResponse.hasFailures()) {
                    logger.info(String.format("Successfully indexed %d features", bulkResponse.getItems() ));
                    listener.onResponse(new AcknowledgedResponse(true));
                    return;
                }
                throw new IllegalStateException(buildBulkRequestFailureMessage(bulkResponse));
            }, e -> { listener.onFailure(e); }));
    }

    private String buildBulkRequestFailureMessage(BulkResponse bulkResponse) {
        List<BulkItemResponse> failedResponse = new ArrayList<>();
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (response.isFailed()) {
                failedResponse.add(response);
            }
        }
        return failedResponse.stream().map(BulkItemResponse::getFailureMessage).collect(Collectors.joining());
    }

    private IndexRequestBuilder createIndexRequestBuilder(Map<String, Object> document) {
        IndexRequestBuilder requestBuilder = client.prepareIndex(content.getIndexName(), MAPPING_TYPE)
            .setSource(document)
            .setPipeline(pipelineId);
        if (!Strings.hasText(content.getFeatureId())) {
            return requestBuilder;
        }
        Object id = GeospatialParser.extractValueAsString(document, content.getFeatureId());
        return id == null ? requestBuilder : requestBuilder.setId(content.getFeatureId());
    }

    private BulkRequestBuilder createBulkRequest() {
        return client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
    }
}
