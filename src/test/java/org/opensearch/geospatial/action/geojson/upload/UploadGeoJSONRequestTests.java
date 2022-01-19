/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.geojson.upload;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class UploadGeoJSONRequestTests extends OpenSearchTestCase {

    private String getRequestBody() {
        return "{}";
    }

    public void testStreams() throws IOException {
        String requestBody = getRequestBody();
        UploadGeoJSONRequest request = new UploadGeoJSONRequest(new BytesArray(requestBody.getBytes(StandardCharsets.UTF_8)));
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);

        UploadGeoJSONRequest serialized = new UploadGeoJSONRequest(in);
        assertEquals(requestBody, serialized.getSource().utf8ToString());
    }

    public void testRequestValidation() {
        UploadGeoJSONRequest request = new UploadGeoJSONRequest(new BytesArray(getRequestBody().getBytes(StandardCharsets.UTF_8)));
        assertNull(request.validate());
    }
}
