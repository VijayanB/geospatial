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

package org.opensearch.geospatial.geojson;


import java.util.Map;

public class GeoJSONFactory {

    public static final String FEATURE = "Feature";
    public static final String GEOMETRY = "geometry";
    public static final String ID = "id";
    public static final String PROPERTIES = "properties";
    public static final String TYPE = "type";

    public static Feature create(Map<String, Object> input) throws IllegalArgumentException {
        String geoJSONType = (String) input.get(TYPE);
        if (geoJSONType == null) {
            throw new IllegalArgumentException(TYPE + " cannot be null");
        }
        if (!FEATURE.equalsIgnoreCase(geoJSONType)) {
            throw new IllegalArgumentException(geoJSONType + " is not supported. Only type " + FEATURE + " is supported");
        }
        return readFeature(input);
    }

    private static Feature readFeature(Map<String, Object> input) {
        Map<String, Object> geometryMap = (Map<String, Object>) input.get(GEOMETRY);
        Feature.FeatureBuilder featureBuilder = new Feature.FeatureBuilder(geometryMap);
        if (input.containsKey(ID)) {
            featureBuilder.id((String) input.get(ID));
        }
        if (input.containsKey(PROPERTIES)) {
            featureBuilder.properties((Map<String, Object>) input.get(PROPERTIES));
        }
        return featureBuilder.build();
    }
}
