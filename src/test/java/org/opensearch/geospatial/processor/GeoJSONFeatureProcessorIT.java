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
import org.opensearch.common.settings.Settings;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public class GeoJSONFeatureProcessorIT extends GeospatialRestTestCase {

    public static final int OBJECT_LENGTH = 10;

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

    public void testIndexGeoJSONSuccess() throws IOException {

        String indexName = randomAlphaOfLength(OBJECT_LENGTH).toLowerCase(Locale.getDefault());
        String geoShapeField = randomAlphaOfLength(OBJECT_LENGTH).toLowerCase(Locale.getDefault());
        String pipelineName = randomAlphaOfLength(OBJECT_LENGTH).toLowerCase(Locale.getDefault());

        Map<String, String> geoFields = new HashMap<>();
        geoFields.put(geoShapeField, "geo_shape");


        Map<String, String> processorProperties = new HashMap<>();
        processorProperties.put(GeoJSONFeatureProcessor.FIELD_KEY, geoShapeField);
        Map<String, Object> geoJSONProcessorConfig = buildGeoJSONProcessorConfig(processorProperties);
        List<Map<String, Object>> configs = new ArrayList<>();
        configs.add(geoJSONProcessorConfig);

        createPipeline(pipelineName, Optional.empty(), configs);

        createIndex(indexName, Settings.EMPTY, geoFields);

        Map<String, Object> properties = new HashMap<>();
        properties.put(randomAlphaOfLength(OBJECT_LENGTH).toLowerCase(Locale.getDefault()), randomAlphaOfLength(OBJECT_LENGTH).toLowerCase(Locale.getDefault()));
        properties.put(randomAlphaOfLength(OBJECT_LENGTH).toLowerCase(Locale.getDefault()), randomBoolean());

        double[][] coordinates = new double[][]{
            {
                randomDouble(), randomDouble()
            },
            {
                randomDouble(), randomDouble()
            },
            {
                randomDouble(), randomDouble()
            },
            {
                randomDouble(), randomDouble()
            }
        };
        String body = buildGeoJSONFeatureAsString("LineString", coordinates, properties);
        Map<String, String> params = new HashMap<>();
        params.put("pipeline", pipelineName);

        String docID = randomAlphaOfLength(OBJECT_LENGTH).toLowerCase(Locale.getDefault());
        indexDocument(indexName, docID, body, params);

        Map<String, Object> document = getIndexDocument(docID, indexName);
        assertNotNull(document);

        for (Map.Entry<String, Object> property : properties.entrySet()) {
            assertEquals(document.get(property.getKey()), property.getValue());
        }

        Map<String, Object> geoShapeFieldValue = (Map<String, Object>) document.get(geoShapeField);
        assertEquals(geoShapeFieldValue.get(GEOMETRY_TYPE_KEY), "LineString");

        deletePipeline(pipelineName);
        deleteIndex(indexName);

    }
}
