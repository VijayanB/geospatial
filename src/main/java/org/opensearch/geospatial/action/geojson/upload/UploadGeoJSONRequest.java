/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.geojson.upload;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.Objects;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;

public class UploadGeoJSONRequest extends ActionRequest {

    private String indexName;

    public UploadGeoJSONRequest(String indexName) {
        this.indexName = Objects.requireNonNull(indexName);
    }

    public UploadGeoJSONRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = new ActionRequestValidationException();
        if (!Strings.hasText(indexName)) {
            validationException = addValidationError("index name cannot be empty", validationException);
        }
        return validationException != null ? validationException: null;
    }
}
