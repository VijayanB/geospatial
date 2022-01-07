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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Feature {
    public static final String TYPE = "Feature";
    private final Map<String, Object> geometry;
    private String id;
    private Map<String, Object> properties = new HashMap<>();

    private Feature(Map<String, Object> geometry) {
        this.geometry = geometry;
    }

    public Map<String, Object> getGeometry() {
        return geometry;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public String getId() {
        return id;
    }

    static class FeatureBuilder {

        private Feature feature;

        public FeatureBuilder(Map<String, Object> geometry) {
            this.feature = new Feature(geometry);
        }

        public Feature build() {
            return feature;
        }

        public FeatureBuilder id(String id) {
            this.feature.id = id;
            return this;
        }

        public FeatureBuilder properties(Map<String, Object> properties) {
            this.feature.properties = properties;
            return this;
        }
    }
}
