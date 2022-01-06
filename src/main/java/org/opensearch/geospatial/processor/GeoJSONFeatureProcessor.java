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

import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

public final class GeoJSONFeatureProcessor extends AbstractProcessor {

    public static final String TYPE = "geo-json";
    private String targetField;

    public GeoJSONFeatureProcessor(String tag, String description, String targetField) {
        super(tag, description);
        this.targetField = targetField;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        Object feature = ingestDocument.getFieldValue("geometry", Object.class, false);
        ingestDocument.setFieldValue(targetField, feature);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements org.opensearch.ingest.Processor.Factory {
        @Override
        public GeoJSONFeatureProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                              String description, Map<String, Object> config) {
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field");

            return new GeoJSONFeatureProcessor(processorTag, description, targetField);
        }
    }
}
