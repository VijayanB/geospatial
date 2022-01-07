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

import org.opensearch.geospatial.geojson.GeoJSONFactory;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class GeoJSONFeatureProcessorTests extends OpenSearchTestCase {

    private Map<String, Object> buildGeoJSON(String type) {
        Map<String, Object> geoJSON = new HashMap<>();
        geoJSON.put("type", type);

        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "Dinagat Islands");
        geoJSON.put("properties", properties);

        Map<String, Object> geometry = new HashMap<>();
        geometry.put("type", "Point");
        geometry.put("coordinates", "[125.6, 10.1]");
        geoJSON.put("geometry", geometry);

        return geoJSON;

    }

    public void testGeoJSONProcessorSuccess() {
        Map<String, Object> document = buildGeoJSON("Feature");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        GeoJSONFeatureProcessor processor = new GeoJSONFeatureProcessor("sample", "description", "location");
        processor.execute(ingestDocument);
        Map<String, Object> location = (Map<String, Object>) ingestDocument.getFieldValue("location", Object.class);
        assertNotNull(location);
        assertEquals(document.get(GeoJSONFactory.GEOJSON_GEOMETRY_KEY), location);
        assertEquals("Dinagat Islands", ingestDocument.getSourceAndMetadata().get("name"));
        assertNull(ingestDocument.getSourceAndMetadata().get(GeoJSONFactory.GEOJSON_GEOMETRY_KEY));
        assertNull(ingestDocument.getSourceAndMetadata().get(GeoJSONFactory.GEOJSON_TYPE_KEY));
        assertNull(ingestDocument.getSourceAndMetadata().get(GeoJSONFactory.GEOJSON_PROPERTIES_KEY));
    }

    public void testGeoJSONProcessorUnSupportedType() {
        Map<String, Object> document = buildGeoJSON("FeatureCollection");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        GeoJSONFeatureProcessor processor = new GeoJSONFeatureProcessor("sample", "description", "location");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertTrue(exception.getMessage().contains("Only type Feature is supported"));
    }

    public void testGeoJSONProcessorTypeNotFound() {
        Map<String, Object> document = buildGeoJSON("Feature");
        document.remove("type");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        GeoJSONFeatureProcessor processor = new GeoJSONFeatureProcessor("sample", "description", "location");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertTrue(exception.getMessage().contains("type cannot be null"));
    }
}
