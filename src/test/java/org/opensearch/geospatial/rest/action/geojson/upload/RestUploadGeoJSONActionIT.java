/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.rest.action.geojson.upload;

import java.io.IOException;
import java.util.Locale;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.plugin.GeospatialPlugin;
import org.opensearch.rest.RestStatus;

import static org.opensearch.ingest.RandomDocumentPicks.randomString;

public class RestUploadGeoJSONActionIT extends GeospatialRestTestCase {

    public void testGeoJSONUploadSuccess() throws IOException {

        String path = String.join(
            RestUploadGeoJSONAction.URL_DELIMITER,
            GeospatialPlugin.getPluginURLPrefix(),
            RestUploadGeoJSONAction.ACTION_OBJECT,
            RestUploadGeoJSONAction.ACTION_UPLOAD
        );
        Request request = new Request("POST", path);
        StringBuilder requestEntity = new StringBuilder();
        requestEntity.append("{");
        requestEntity.append("\"index\": ");
        requestEntity.append("\"some-index\"");
        requestEntity.append("}");
        request.setJsonEntity(requestEntity.toString());
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

    }
}
