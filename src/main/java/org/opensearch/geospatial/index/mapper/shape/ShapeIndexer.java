/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.shape;

import static org.opensearch.geometry.ShapeType.CIRCLE;
import static org.opensearch.geometry.ShapeType.LINEARRING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.IndexableField;
import org.opensearch.geometry.Circle;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.GeometryCollection;
import org.opensearch.geometry.GeometryVisitor;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.LinearRing;
import org.opensearch.geometry.MultiLine;
import org.opensearch.geometry.MultiPoint;
import org.opensearch.geometry.MultiPolygon;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geometry.ShapeType;
import org.opensearch.geospatial.index.common.shape.ShapeConverter;
import org.opensearch.index.mapper.AbstractGeometryFieldMapper;
import org.opensearch.index.mapper.ParseContext;

/**
 * Converts geometries into Lucene-compatible form for indexing in a shape field.
 */
public class ShapeIndexer implements AbstractGeometryFieldMapper.Indexer<Geometry, Geometry> {

    private final String name;
    private final GeometryVisitor<IndexableField[], RuntimeException> visitor;

    public ShapeIndexer(String name) {
        this.name = Objects.requireNonNull(name, "name cannot be null");
        visitor = new XYShapeGeometryVisitor(name);
    }

    @Override
    public Geometry prepareForIndexing(Geometry geometry) {

        if (geometry == null) {
            return null;
        }
        return geometry.visit(new GeometryVisitor<>() {
            public Geometry visit(Circle circle) {
                throw new UnsupportedOperationException(CIRCLE + " is not supported");
            }

            public Geometry visit(GeometryCollection<?> collection) {
                return collection;
            }

            public Geometry visit(Line line) {
                return line;
            }

            public Geometry visit(LinearRing ring) {
                throw new UnsupportedOperationException(String.format("cannot index %s [ %s ] directly", LINEARRING, ring));
            }

            public Geometry visit(MultiLine multiLine) {
                return multiLine;
            }

            public Geometry visit(MultiPoint multiPoint) {
                return multiPoint;
            }

            public Geometry visit(MultiPolygon multiPolygon) {
                return multiPolygon;
            }

            public Geometry visit(Point point) {
                return point;
            }

            public Geometry visit(Polygon polygon) {
                return polygon;
            }

            public Geometry visit(Rectangle rectangle) {
                return rectangle;
            }
        });
    }

    @Override
    public Class<Geometry> processedClass() {
        return Geometry.class;
    }

    @Override
    public List<IndexableField> indexShape(ParseContext parseContext, Geometry geometry) {
        return Arrays.asList(geometry.visit(visitor));
    }

    // Visitor to build Shapes into Lucene indexable fields
    private static class XYShapeGeometryVisitor implements GeometryVisitor<IndexableField[], RuntimeException> {
        private final String name;

        private XYShapeGeometryVisitor(String name) {
            this.name = name;
        }

        @Override
        public IndexableField[] visit(Circle circle) {
            throw new IllegalArgumentException(String.format("invalid shape type found [ %s ] while indexing shape", CIRCLE));
        }

        private IndexableField[] visitCollection(GeometryCollection<?> collection) {
            List<IndexableField> fields = new ArrayList<>();
            collection.forEach(geometry -> fields.addAll(Arrays.asList(geometry.visit(this))));
            return fields.toArray(IndexableField[]::new);
        }

        @Override
        public IndexableField[] visit(GeometryCollection<?> collection) {
            Objects.requireNonNull(collection, String.format("%s cannot be null", ShapeType.GEOMETRYCOLLECTION));
            return visitCollection(collection);
        }

        @Override
        public IndexableField[] visit(Line line) {
            Objects.requireNonNull(line, String.format("%s cannot be null", ShapeType.LINESTRING));
            XYLine cartesianLine = ShapeConverter.toXYLine(line);
            return XYShape.createIndexableFields(name, cartesianLine);
        }

        public IndexableField[] visit(LinearRing ring) {
            throw new IllegalArgumentException(String.format("invalid shape type found [ %s ] while indexing shape", LINEARRING));
        }

        public IndexableField[] visit(MultiLine multiLine) {
            Objects.requireNonNull(multiLine, String.format("%s cannot be null", ShapeType.MULTILINESTRING));
            return visitCollection(multiLine);
        }

        public IndexableField[] visit(MultiPoint multiPoint) {
            Objects.requireNonNull(multiPoint, String.format("%s cannot be null", ShapeType.MULTIPOINT));
            return visitCollection(multiPoint);
        }

        public IndexableField[] visit(MultiPolygon multiPolygon) {
            Objects.requireNonNull(multiPolygon, String.format("%scannot be null", ShapeType.MULTIPOLYGON));
            return visitCollection(multiPolygon);
        }

        public IndexableField[] visit(Point point) {
            Objects.requireNonNull(point, String.format("%s cannot be null", ShapeType.POINT));
            float x = Double.valueOf(point.getX()).floatValue();
            float y = Double.valueOf(point.getY()).floatValue();
            return XYShape.createIndexableFields(name, x, y);
        }

        private IndexableField[] createIndexableFields(XYPolygon polygon) {
            return XYShape.createIndexableFields(name, polygon);
        }

        public IndexableField[] visit(Polygon polygon) {
            Objects.requireNonNull(polygon, String.format("%s cannot be null", ShapeType.POLYGON));
            XYPolygon luceneXYPolygon = ShapeConverter.toXYPolygon(polygon);
            return createIndexableFields(luceneXYPolygon);
        }

        public IndexableField[] visit(Rectangle rectangle) {
            Objects.requireNonNull(rectangle, "Rectangle cannot be null");
            XYPolygon luceneXYPolygon = ShapeConverter.toXYPolygon(rectangle);
            return createIndexableFields(luceneXYPolygon);
        }
    }
}
