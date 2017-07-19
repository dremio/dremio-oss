/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic;

import java.util.Set;

import javax.xml.bind.DatatypeConverter;

import com.dremio.common.expression.SchemaPath;
import com.dremio.plugins.Version;
import com.google.common.collect.ImmutableSet;

/**
 * Common constant values
 */
public interface ElasticsearchConstants {
  // Defaults
  int ES_CONFIG_DEFAULT_BATCH_SIZE = 4000;
  int DEFAULT_SCROLL_TIMEOUT_MILLIS = 300 * 1000;
  int DEFAULT_READ_TIMEOUT_MILLIS = 60 * 1000;

  public static final Version VERSION_0_0 = new Version(0, 0, 0);

  /* Elasticsearch keywords */
  String SCROLL_ID = "_scroll_id";
  String HITS = "hits";
  String INDEX = "_index";
  String TYPE = "_type";
  String FIELDS = "fields";
  String SOURCE_PAINLESS = "params._source";
  String SOURCE_GROOVY = "_source";
  String SOURCE = SOURCE_GROOVY;
  String ID = "_id";
  String UID = "_uid";
  String TOTAL_HITS = "total";
  String DOC = "doc";

  String STRICT = "strict_";

  /* Elastic dremio internal constants */
  String NULL_BOOLEAN_TAG = "NULL_BOOLEAN_TAG";
  String NULL_BYTE_TAG = DatatypeConverter.printBase64Binary("NULL_BINARY_TAG".getBytes());
  String NULL_STRING_TAG = "NULL_STRING_TAG";
  Integer NULL_INTEGER_TAG = Integer.MIN_VALUE;
  Long NULL_LONG_TAG = Long.MIN_VALUE;
  Float NULL_FLOAT_TAG = Float.MIN_VALUE;
  Double NULL_DOUBLE_TAG = Double.MIN_VALUE;
  Long NULL_TIME_TAG = 1L;

  /* Aggregation pushdown operations supported */
  String AGG_SUM = "SUM";
  String AGG_SUM0 = "$SUM0";
  String AGG_COUNT = "COUNT";
  String AGG_MIN = "MIN";
  String AGG_MAX = "MAX";
  String AGG_AVG = "AVG";
  String AGG_STDDEV_POP = "STDDEV_POP";
  String AGG_STDDEV_SAMP = "STDDEV_SAMP";
  String AGG_STDDEV = "STDDEV";
  String AGG_VAR_POP = "VAR_POP";
  String AGG_VAR_SAMP = "VAR_SAMP";
  String AGG_VAR = "VARIANCE";

  String GEO_POINT_LAT = "lat";
  String GEO_POINT_LON = "lon";
  String GEO_SHAPE_TYPE = "type";
  String GEO_SHAPE_COORDINATES = "coordinates";
  String GEO_SHAPE_GEOMETRIES = "geometries";
  String GEO_SHAPE_ORIENTATION = "orientation";
  String GEO_SHAPE_RADIUS = "radius";

  Set<String> META_COLUMNS = ImmutableSet.of(UID, ID, TYPE, INDEX);
  Set<SchemaPath> META_PATHS = ImmutableSet.of(
      SchemaPath.getSimplePath(UID),
      SchemaPath.getSimplePath(ID),
      SchemaPath.getSimplePath(TYPE),
      SchemaPath.getSimplePath(INDEX)
      );
}
