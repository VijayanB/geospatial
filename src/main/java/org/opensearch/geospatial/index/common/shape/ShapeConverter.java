/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.common.shape;

import static org.apache.commons.lang3.ArrayUtils.toPrimitive;

import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.opensearch.geometry.Line;
import org.opensearch.geometry.Polygon;
import org.opensearch.geometry.Rectangle;

public class ShapeConverter {

    public static XYLine toXYLine(Line line) {
        float[] x = toFloatArray(line.getX());
        float[] y = toFloatArray(line.getY());
        return new XYLine(x, y);
    }

    private static float[] toFloatArray(double[] input) {
        final Float[] floats = DoubleStream.of(input).boxed().map(Double::floatValue).toArray(Float[]::new);
        return toPrimitive(floats);
    }

    public static XYPolygon toXYPolygon(Rectangle r) {
        // build polygon by assigning points in Counter Clock Wise direction (default for polygon) and end at where
        // you started since it has to be linear ring
        // (minX,minY) -> (maxX+minY) -> (maxX, maxY) -> (minX, maxY) -> (minX,minY)
        double[] x = new double[] { r.getMinX(), r.getMaxX(), r.getMaxX(), r.getMinX(), r.getMinX() };
        double[] y = new double[] { r.getMinY(), r.getMinY(), r.getMaxY(), r.getMaxY(), r.getMinY() };
        return new XYPolygon(toFloatArray(x), toFloatArray(y));
    }

    public static XYPolygon toXYPolygon(Polygon polygon) {

        XYPolygon[] holes = buildXYPolygonFromHoles(polygon);

        Line line = polygon.getPolygon();
        XYLine polygonLine = toXYLine(line);

        return new XYPolygon(polygonLine.getX(), polygonLine.getY(), holes);
    }

    private static XYPolygon[] buildXYPolygonFromHoles(final Polygon polygon) {
        return IntStream.range(0, polygon.getNumberOfHoles())
            .mapToObj(i -> polygon.getHole(i))
            .map(ShapeConverter::toXYLine)
            .map(line -> new XYPolygon(line.getX(), line.getY()))
            .collect(Collectors.toList())
            .toArray(XYPolygon[]::new);

    }
}
