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


import org.opensearch.geospatial.geojson.Feature;
import org.opensearch.geospatial.geojson.GeoJSONFactory;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

public class GeoJSONFeatureProcessor extends AbstractProcessor {

    public static final String FIELD = "field";
    public static final String TYPE = "geojson";
    private final String geoShapeField;

    public GeoJSONFeatureProcessor(String tag, String description, String geoShapeField) {
        super(tag, description);
        this.geoShapeField = geoShapeField;
    }


    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        Feature feature = GeoJSONFactory.create(ingestDocument.getSourceAndMetadata());
        ingestDocument.removeField(GeoJSONFactory.TYPE);
        feature.getProperties().forEach((k, v) -> ingestDocument.setFieldValue(k, v));
        ingestDocument.removeField(GeoJSONFactory.PROPERTIES);
        ingestDocument.setFieldValue(this.geoShapeField, feature.getGeometry());
        ingestDocument.removeField(GeoJSONFactory.GEOMETRY);
        if (feature.getId() != null && feature.getId().trim().length() > 0) {
            ingestDocument.appendFieldValue("_id", feature.getId());
            ingestDocument.removeField(GeoJSONFactory.ID);
        }
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
            String geoShapeField = readStringProperty(TYPE, processorTag, config, FIELD);
            return new GeoJSONFeatureProcessor(processorTag, description, geoShapeField);
        }
    }
}
