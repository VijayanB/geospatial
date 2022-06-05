/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.shape;

import org.apache.lucene.search.Query;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.geometry.Geometry;
import org.opensearch.index.query.QueryShardContext;

public interface ShapeQueryable {
    Query shapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context);
}
