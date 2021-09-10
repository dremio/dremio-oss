/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

  public static final Version MIN_ELASTICSEARCH_VERSION = new Version(2, 0, 0);

  // Defines a cutoff for enabling newer features that have been added to elasticsearch. Rather than
  // maintain an exact matrix of what is supported, We simply try to make use of all features available
  // above this version and disable them for connections to any version below. This cutoff is inclusive
  // on the ENABLE side so all new features must be available in 2.1.2 and up. Everything missing in
  // a version between 2.0 and 2.1.1 is disabled for all versions in that range.
  public static final Version MIN_VERSION_TO_ENABLE_NEW_FEATURES = new Version(2, 1, 2);

  // Version 5x or higher
  public static final Version ELASTICSEARCH_VERSION_5X = new Version(5, 0, 0);

  //Version 5.3.x or higher
  public static final Version ELASTICSEARCH_VERSION_5_3_X = new Version(5, 3, 0);

  // Version 6.0.x or higher
  public static final Version ELASTICSEARCH_VERSION_DEFAULT = new Version(6, 0, 0);

  // Version 6.8.x or higher
  public static final Version ELASTICSEARCH_VERSION_6_8_X = new Version(6, 8, 0);

  //Version 7.0.x or higher
  public static final Version ELASTICSEARCH_VERSION_7_0_X = new Version(7, 0, 0);

  // Elasticsearch formats
  public static final String ES_GENERIC_FORMAT1 = "[ddMMMyyyy:HH:mm:ss";
  public static final String ES_GENERIC_FORMAT2 = "[ ][Z][X]]";
  public static final String ES_GENERIC_FORMAT3 = "[yyyy[-][/]MM[-][/]dd['T'][ ]HH:mm[:][.]ss";
  public static final String ES_GENERIC_FORMAT4 = "[Z][X]]";
  public static final String ES_GENERIC_FORMAT5 = "[EEE, dd MMM yyyy HH:mm:ss zzz]";
  public static final String ES_GENERIC_FORMAT6 =  "[yyyyMMdd['T']HHmmss[.][SSS][zzz]]";
  public static final String ES_GENERIC_FORMAT7 = "[yyyy[-][/][MM]]";
  public static final String ES_GENERIC_FORMAT8 =  "[yyyy[-][/]DDD['T']HH[:]mm[:]sszz]";
  public static final String ES_GENERIC_FORMAT9 = "[HH[:]mm[:]ss[.][SSS][Z]]";
  public static final String ES_TIME_FORMAT = "[['T']HH[:]mm[:]ss[.][SSS][z]]";

  // TimeUnit constants
  public static final long MILLIS_PER_SECOND = 1000L;
  public static final long MILLIS_PER_DAY = 24 * 60 * 60 * MILLIS_PER_SECOND;
  public static final long NANOS_PER_MILLISECOND_LONG = 1000000L;

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
  String TOTAL_HITS_VALUE = "value";
  String RELATION = "relation";
  String DOC = "doc";

  String DISABLE_COORD_FIELD = "\"disable_coord\" : false,";
  String USE_DIS_MAX = "\"use_dis_max\" : true,";
  String AUTO_GENERATE_PHRASE_QUERIES = "\"auto_generate_phrase_queries\" : false,";
  String SPLIT_ON_WHITESPACE = "\"split_on_whitespace\" : true,";
  static final String NESTED_TYPE_DATA = "/json/smoke-test/";

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
