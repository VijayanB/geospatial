/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.geometry.Geometry;
import org.opensearch.index.query.QueryShardContext;

// A query that matches documents based on ShapeRelation
public class ShapeQueryProcessor {

    public Query shapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        // TODO: Return only docs that matches relation
        return new MatchAllDocsQuery();
    }
}
