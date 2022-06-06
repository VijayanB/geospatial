/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.shape;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.opensearch.common.Explicit;
import org.opensearch.common.geo.GeometryParser;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.geometry.Geometry;
import org.opensearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.opensearch.index.mapper.GeoShapeParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.VectorGeoShapeQueryProcessor;

public class ShapeFieldMapper extends AbstractShapeGeometryFieldMapper<Geometry, Geometry> {

    public static final String CONTENT_TYPE = "shape";
    public static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setDimensions(7, 4, Integer.BYTES);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.freeze();
    }

    protected ShapeFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        Explicit ignoreMalformed,
        Explicit coerce,
        Explicit ignoreZValue,
        Explicit orientation,
        MultiFields multiFields,
        CopyTo copyTo
    ) {
        super(simpleName, fieldType, mappedFieldType, ignoreMalformed, coerce, ignoreZValue, orientation, multiFields, copyTo);
    }

    public static class Builder extends AbstractShapeGeometryFieldMapper.Builder<Builder, ShapeFieldType> {

        public Builder(String name) {
            super(name, FIELD_TYPE);
            this.hasDocValues = false;
        }

        private ShapeFieldType buildShapeFieldType(BuilderContext context) {
            ShapeFieldType fieldType = new ShapeFieldType(buildFullName(context), indexed, this.fieldType.stored(), hasDocValues, meta);
            GeometryParser geometryParser = new GeometryParser(
                orientation().value().getAsBoolean(),
                coerce().value(),
                ignoreZValue().value()
            );
            fieldType.setGeometryParser(new GeoShapeParser(geometryParser));
            fieldType.setGeometryIndexer(new ShapeIndexer(buildFullName(context)));
            fieldType.setOrientation(orientation().value());
            return fieldType;
        }

        @Override
        public ShapeFieldMapper build(BuilderContext context) {
            return new ShapeFieldMapper(
                name,
                fieldType,
                buildShapeFieldType(context),
                ignoreMalformed(context),
                coerce(context),
                ignoreZValue(),
                orientation(),
                multiFieldsBuilder.build(this, context),
                copyTo
            );
        }
    }

    @Override
    protected void mergeGeoOptions(AbstractShapeGeometryFieldMapper mergeWith, List conflicts) {
        return;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void addStoredFields(ParseContext context, Geometry geometry) {

    }

    @Override
    protected void addDocValuesFields(String name, Geometry geometry, List<IndexableField> fields, ParseContext context) {

    }

    @Override
    protected void addMultiFields(ParseContext context, Geometry geometry) throws IOException {

    }

    public static class ShapeFieldType extends AbstractShapeGeometryFieldType<Geometry, Geometry> implements ShapeQueryable {

        // TODO: Replace with ShapeQueryProcessor
        private final VectorGeoShapeQueryProcessor queryProcessor;

        public ShapeFieldType(String name, boolean indexed, boolean stored, boolean hasDocValues, Map<String, String> meta) {
            super(name, indexed, stored, hasDocValues, false, meta);
            this.queryProcessor = new VectorGeoShapeQueryProcessor();
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query shapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
            return queryProcessor.geoShapeQuery(shape, fieldName, relation, context);
        }
    }

    public static final class TypeParser extends AbstractShapeGeometryFieldMapper.TypeParser {

        @Override
        protected AbstractShapeGeometryFieldMapper.Builder newBuilder(String name, Map<String, Object> params) {
            return new Builder(name);
        }

        @Override
        protected boolean parseXContentParameters(String name, Map.Entry<String, Object> entry, Map<String, Object> params)
            throws MapperParsingException {
            return false;
        }
    }

    @Override
    public ShapeFieldType fieldType() {
        return (ShapeFieldType) super.fieldType();
    }

    @Override
    protected boolean docValuesByDefault() {
        return false;
    }

}
