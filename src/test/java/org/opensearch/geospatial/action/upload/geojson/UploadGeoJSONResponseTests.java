/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import org.json.JSONObject;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

public class UploadGeoJSONResponseTests extends OpenSearchTestCase {


//    public void testStreams() throws IOException {
//        UploadGeoJSONResponse request = new UploadGeoJSONResponse(new BulkResponse(null, 1L, 1L));
//        BytesStreamOutput output = new BytesStreamOutput();
//        request.writeTo(output);
//        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);
//
//        UploadGeoJSONResponse serialized = new UploadGeoJSONResponse(in);
//        assertEquals(request, serialized);
//    }
}
