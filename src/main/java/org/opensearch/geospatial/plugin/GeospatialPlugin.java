/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.geospatial.plugin;

import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.geo.builders.ShapeBuilder;

import org.opensearch.common.collect.MapBuilder;
import org.opensearch.geospatial.processor.GeoJSONFeatureProcessor;
import org.opensearch.ingest.Processor.Factory;
import org.opensearch.ingest.Processor.Parameters;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.Plugin;

import java.util.Map;

public class GeospatialPlugin extends Plugin implements IngestPlugin {

    @Override
    public Map<String, Factory> getProcessors(Parameters parameters) {
        return MapBuilder.<String, Factory>newMapBuilder()
            .put(GeoJSONFeatureProcessor.TYPE, new GeoJSONFeatureProcessor.Factory())
            .immutableMap();
    }
}
