/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.geojson.upload;

import java.io.IOException;
import java.util.Objects;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class UploadGeoJSONRequest extends ActionRequest {

    private BytesReference source;

    public UploadGeoJSONRequest(BytesReference source) {
        this.source = Objects.requireNonNull(source);
    }

    public UploadGeoJSONRequest(StreamInput in) throws IOException {
        super(in);
        this.source = in.readBytesReference();
    }

    public BytesReference getSource() {
        return source;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(source);
    }
}
