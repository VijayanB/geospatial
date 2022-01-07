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

import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeoJSONFeatureProcessorTests extends OpenSearchTestCase {

    public void testGeoJSONProcessorSuccess() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("repository", "geospatial");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        GeoJSONFeatureProcessor processor = new GeoJSONFeatureProcessor("sample", "description");
        processor.execute(ingestDocument);
        Map<String, Object> data = ingestDocument.getSourceAndMetadata();
        assertTrue(data.get("type") instanceof List);
        @SuppressWarnings("unchecked")
        List<String> values = (List<String>) data.get("type");
        assertTrue(values.contains("feature"));
    }
}
