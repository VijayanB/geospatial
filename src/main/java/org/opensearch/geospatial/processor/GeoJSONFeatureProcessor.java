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
import org.opensearch.geospatial.geojson.Feature;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;


import java.util.Map;

public class GeoJSONFeatureProcessor extends AbstractProcessor {

    public static final String FIELD_KEY = "field";
    public static final String TYPE = "geojson";
    private final String geoShapeField;

    public GeoJSONFeatureProcessor(String tag, String description, String geoShapeField) {
        super(tag, description);
        this.geoShapeField = geoShapeField;
    }


    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        Feature feature = GeoJSONFactory.create(ingestDocument.getSourceAndMetadata());
        ingestDocument.removeField(GeoJSONFactory.GEOJSON_TYPE_KEY);
        feature.getProperties().forEach((k,v) -> ingestDocument.setFieldValue(k, v));
        ingestDocument.removeField(GeoJSONFactory.GEOJSON_PROPERTIES_KEY);
        ingestDocument.setFieldValue(this.geoShapeField, feature.getGeometry());
        ingestDocument.removeField(GeoJSONFactory.GEOJSON_GEOMETRY_KEY);
        if(feature.getId() != null && feature.getId().trim().length() > 0){
            ingestDocument.setFieldValue("_id", feature.getId());
            ingestDocument.removeField(GeoJSONFactory.GEOJSON_ID_KEY);
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
            String geoShapeField = readStringProperty(TYPE, processorTag, config, FIELD_KEY);
            return new GeoJSONFeatureProcessor(processorTag, description, geoShapeField);
        }
    }
}
