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
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.action.ActionResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class UploadGeoJSONResponse extends ActionResponse implements ToXContentObject {

    private final BulkResponse response;

    public UploadGeoJSONResponse(BulkResponse response) {
        this.response = response;
    }

    public UploadGeoJSONResponse(StreamInput in) throws IOException {
        super(in);
        this.response = new BulkResponse(in);
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        this.response.writeTo(streamOutput);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("acknowledged", true);
        builder.field("took_in_ms", response.getTook().getMillis());
        builder.field("errors", response.hasFailures());
        builder.field("total", response.getItems().length);
        if (!response.hasFailures()) {
            return buildSuccessXContent(builder);
        }
        return buildFailureXContent(builder);
    }

    private XContentBuilder buildSuccessXContent(XContentBuilder builder) throws IOException {
        builder.field("success", response.getItems().length);
        builder.field("failure", 0);
        return builder.endObject();
    }

    private XContentBuilder buildFailureXContent(XContentBuilder builder) throws IOException {
        final Map<String, String> failedResponses = Arrays.stream(response.getItems())
            .filter(BulkItemResponse::isFailed)
            .collect(Collectors.toMap(BulkItemResponse::getId, BulkItemResponse::getFailureMessage));
        builder.field("success", response.getItems().length - failedResponses.size());

        builder.field("failure", failedResponses.size());
        builder.startArray("messages");
        for (Map.Entry<String, String> entry : failedResponses.entrySet()) {
            builder.startObject();
            builder.field("id", entry.getKey());
            builder.field("reason", entry.getValue());
            builder.endObject();
        }
        builder.endArray();
        return builder.endObject();
    }
}
