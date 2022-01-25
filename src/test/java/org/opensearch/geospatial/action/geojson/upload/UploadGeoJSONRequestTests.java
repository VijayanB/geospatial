/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.geojson.upload;

import static org.opensearch.ingest.RandomDocumentPicks.randomString;

import java.io.IOException;
import java.util.Locale;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class UploadGeoJSONRequestTests extends OpenSearchTestCase {

    private String genereateIndexName() {
        return randomString(random()).toLowerCase(Locale.getDefault());
    }

    public void testStreams() throws IOException {
        String indexName = genereateIndexName();
        UploadGeoJSONRequest request = new UploadGeoJSONRequest(indexName);
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);

        UploadGeoJSONRequest serialized = new UploadGeoJSONRequest(in);
        assertEquals(indexName, serialized.getIndexName());
    }

    public void testRequestValidationSucceed() {
        UploadGeoJSONRequest request = new UploadGeoJSONRequest(genereateIndexName());
        ActionRequestValidationException validate = request.validate();
        assertNotNull(validate);
    }

    public void testRequestValidationInvalidIndexName() {
        UploadGeoJSONRequest request = new UploadGeoJSONRequest("");
        ActionRequestValidationException validate = request.validate();
        MatcherAssert.assertThat(
            "error message is not valid",
            validate.validationErrors(),
            Matchers.contains("index name cannot be empty")
        );
    }
}
