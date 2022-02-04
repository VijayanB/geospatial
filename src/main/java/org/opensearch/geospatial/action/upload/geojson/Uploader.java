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

import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.index.IndexRequestBuilder;
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

    public void upload() throws IOException {
        // Parse content, get features, build Bulk Index Request and execute
        ActionListener<AcknowledgedResponse> buildIndexGeoJSONObject = wrapBuildIndexGeoJSON();
        if (!shouldCreateIndex) {
            createPipeline(buildIndexGeoJSONObject);
            return;
        }

        // Create Pipeline with a processor of type "geojson-feature"
        ActionListener<CreateIndexResponse> buildPipeline = wrapBuildPipeline(buildIndexGeoJSONObject);

        // create index
        createIndex(buildPipeline);
    }

    private XContentBuilder buildPipelineBodyWithFeatureProcessor() throws IOException {
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

    private void createPipeline(ActionListener<AcknowledgedResponse> onResponseListener) throws IOException {
        XContentBuilder pipelineRequestXContent = buildPipelineBodyWithFeatureProcessor();
        BytesReference pipelineRequestBodyBytes = BytesReference.bytes(pipelineRequestXContent);
        PutPipelineRequest pipelineRequest = new PutPipelineRequest(pipelineId, pipelineRequestBodyBytes, XContentType.JSON);
        client.admin().cluster().putPipeline(pipelineRequest, onResponseListener);
    }

    private ActionListener<CreateIndexResponse> wrapBuildPipeline(ActionListener<AcknowledgedResponse> onResponseListener) {
        return ActionListener.wrap(notUsed -> { createPipeline(onResponseListener); }, listener::onFailure);
    }

    private void createIndex(ActionListener<CreateIndexResponse> onResponseListener) throws IOException {
        XContentBuilder mapping = buildMappingForIndex();
        CreateIndexRequest request = new CreateIndexRequest(content.getIndexName()).mapping(MAPPING_TYPE, mapping);
        client.admin()
            .indices()
            .create(request, ActionListener.delegateResponse(onResponseListener, (objectActionListener, e) -> { listener.onFailure(e); }));
    }

    private XContentBuilder buildMappingForIndex() throws IOException {
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

    private ActionListener<AcknowledgedResponse> wrapBuildIndexGeoJSON() {
        return ActionListener.wrap(input -> {
            BulkRequestBuilder builder = buildBulkRequestBuilder();
            builder.execute(ActionListener.wrap(bulkResponse -> {
                if (!bulkResponse.hasFailures()) {
                    listener.onResponse(new AcknowledgedResponse(true));
                    return;
                }
                List<BulkItemResponse> failedResponse = new ArrayList<>();
                for (BulkItemResponse response : bulkResponse.getItems()) {
                    if (response.isFailed()) {
                        failedResponse.add(response);
                    }
                }
                String errorMessage = failedResponse.stream().map(BulkItemResponse::getFailureMessage).collect(Collectors.joining());
                listener.onFailure(new IllegalStateException(errorMessage));
            }, e -> { listener.onFailure(e); }));

        }, listener::onFailure);
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
