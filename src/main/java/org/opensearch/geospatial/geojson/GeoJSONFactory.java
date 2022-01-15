/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.geojson;


import java.util.Map;

public class GeoJSONFactory {

    public static final String GEOJSON_GEOMETRY_KEY = "geometry";
    public static final String GEOJSON_ID_KEY = "id";
    public static final String GEOJSON_PROPERTIES_KEY = "properties";
    public static final String GEOJSON_TYPE_KEY = "type";

    public static Feature create(Map<String, Object> input) throws IllegalArgumentException {
        String geoJSONType = (String) input.get(GEOJSON_TYPE_KEY);
        if (geoJSONType == null) {
            throw new IllegalArgumentException(GEOJSON_TYPE_KEY + " cannot be null");
        }
        if (!Feature.TYPE.equalsIgnoreCase(geoJSONType)) {
            throw new IllegalArgumentException(geoJSONType + " is not supported. Only type " + Feature.TYPE + " is supported");
        }
        return readFeature(input);
    }

    private static Feature readFeature(Map<String, Object> input) {
        Map<String, Object> geometryMap = (Map<String, Object>) input.get(GEOJSON_GEOMETRY_KEY);
        Feature.FeatureBuilder featureBuilder = new Feature.FeatureBuilder(geometryMap);
        if (input.containsKey(GEOJSON_ID_KEY)) {
            featureBuilder.id((String) input.get(GEOJSON_ID_KEY));
        }
        if (input.containsKey(GEOJSON_PROPERTIES_KEY)) {
            featureBuilder.properties((Map<String, Object>) input.get(GEOJSON_PROPERTIES_KEY));
        }
        return featureBuilder.build();
    }
}
