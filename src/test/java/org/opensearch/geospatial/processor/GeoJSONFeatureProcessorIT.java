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

package org.opensearch.geospatial.processor;

import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;

public class GeoJSONFeatureProcessorIT extends OpenSearchRestTestCase {

    public void testProcessorAvailable() throws IOException {
        String nodeIngestURL = String.join("/", "_nodes", "ingest");
        String endpoint = nodeIngestURL + "?filter_path=nodes.*.ingest.processors&pretty";
        Request request = new Request("GET", endpoint);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        assertNotNull(responseBody);
        assertTrue(responseBody.contains(GeoJSONFeatureProcessor.TYPE));
    }
}
