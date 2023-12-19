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

import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.reader.FieldReader;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.easy.json.reader.BaseJsonProcessor;
import com.dremio.plugins.Version;
import com.dremio.plugins.elastic.execution.CountingElasticSearch7JsonReader;
import com.dremio.plugins.elastic.execution.CountingElasticsearchJsonReader;
import com.dremio.plugins.elastic.execution.ElasticSearch7JsonReader;
import com.dremio.plugins.elastic.execution.ElasticsearchJsonReader;
import com.dremio.plugins.elastic.execution.FieldReadDefinition;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.gson.JsonObject;
import com.google.gson.internal.LazilyParsedNumber;

public class ElasticVersionBehaviorProvider {

  private final boolean enable7vFeatures;
  private final boolean es5Version;
  private final boolean es68Version;

  private final int elasticMajorVersion;

  public ElasticVersionBehaviorProvider(Version esVersionInCluster) {
    es5Version = esVersionInCluster.compareTo(ElasticsearchConstants.ELASTICSEARCH_VERSION_5X) >= 0;
    enable7vFeatures = esVersionInCluster.compareTo(ElasticsearchConstants.ELASTICSEARCH_VERSION_7_0_X) >= 0;
    es68Version = esVersionInCluster.compareTo(ElasticsearchConstants.ELASTICSEARCH_VERSION_6_8_X) >= 0;
    elasticMajorVersion = esVersionInCluster.getMajor();
  }

  public BaseJsonProcessor createCountingElasticSearchReader(ArrowBuf managedBuf, List<SchemaPath> columns, String resource,
                                                             FieldReadDefinition readDefinition, boolean usingElasticProjection, boolean metaUIDSelected, boolean metaIDSelected,
                                                             boolean metaTypeSelected, boolean metaIndexSelected) {
    if (enable7vFeatures) {
      return new CountingElasticSearch7JsonReader(
        managedBuf,
        columns,
        resource,
        readDefinition,
        usingElasticProjection,
        metaUIDSelected,
        metaIDSelected,
        metaTypeSelected,
        metaIndexSelected
      );
    }

    return new CountingElasticsearchJsonReader(
      managedBuf,
      columns,
      resource,
      readDefinition,
      usingElasticProjection,
      metaUIDSelected,
      metaIDSelected,
      metaTypeSelected,
      metaIndexSelected
    );
  }

  public BaseJsonProcessor createElasticSearchReader(ArrowBuf managedBuf, List<SchemaPath> columns, String resource,
                                                     FieldReadDefinition readDefinition, boolean usingElasticProjection, boolean metaUIDSelected, boolean metaIDSelected,
                                                     boolean metaTypeSelected, boolean metaIndexSelected) {
    if (enable7vFeatures) {
      return new ElasticSearch7JsonReader(
        managedBuf,
        columns,
        resource,
        readDefinition,
        usingElasticProjection,
        metaUIDSelected,
        metaIDSelected,
        metaTypeSelected,
        metaIndexSelected
      );
    }

    return new ElasticsearchJsonReader(
      managedBuf,
      columns,
      resource,
      readDefinition,
      usingElasticProjection,
      metaUIDSelected,
      metaIDSelected,
      metaTypeSelected,
      metaIndexSelected
    );
  }

  public String processElasticSearchQuery(String query) {
    if (enable7vFeatures) {
      query = query.replaceAll(ElasticsearchConstants.DISABLE_COORD_FIELD, "");
      query = query.replaceAll(ElasticsearchConstants.USE_DIS_MAX, "");
      query = query.replaceAll(ElasticsearchConstants.AUTO_GENERATE_PHRASE_QUERIES, "");
      query = query.replaceAll(ElasticsearchConstants.SPLIT_ON_WHITESPACE, "");
    }
    return query;
  }

  public DateFormats.AbstractFormatterAndType[] getWriteHolderForVersion(List<String> formats) {
    if (enable7vFeatures) {
      return getFormatterTypeArr(formats, DateFormats.FormatterAndTypeJavaTime::getFormatterAndType);
    } else if (es68Version) {
      return getFormatterTypeArr(formats, DateFormats.FormatterAndTypeMix::getFormatterAndType);
    } else {
      return getFormatterTypeArr(formats, DateFormats.FormatterAndType::getFormatterAndType);
    }
  }

  public DateFormats.AbstractFormatterAndType getFormatterForVersion() {
    if (enable7vFeatures) {
      return DateFormats.FormatterAndTypeJavaTime.DEFAULT_FORMATTERS_JAVA_TIME[0];
    } else if (es68Version) {
      return DateFormats.FormatterAndTypeMix.DEFAULT_FORMATTERS_MIX[0];
    } else {
      return DateFormats.FormatterAndType.DEFAULT_FORMATTERS[0];
    }
  }

  public long getMillisGenericFormatter(String dateTime) {
    if (isEnable7vFeatures()) {
      return DateFormats.FormatterAndTypeJavaTime.getMillisGenericFormatter(dateTime);
    } else if (isEs68Version()) {
      return DateFormats.FormatterAndTypeMix.getMillisGenericFormatter(dateTime);
    } else {
      return DateFormats.FormatterAndType.getMillisGenericFormatter(dateTime);
    }
  }

  public boolean isEnable7vFeatures() {
    return enable7vFeatures;
  }

  public boolean isEs5Version() {
    return es5Version;
  }

  public boolean isEs68Version() {
    return es68Version;
  }

  public int geMajorVersion() {
    return elasticMajorVersion;
  }

  public int readTotalResultReader(FieldReader totalResultReader) {
    if (!enable7vFeatures) {
      return readAsInt(totalResultReader.reader("hits").reader("total").readText().toString());
    }
    return readAsInt(totalResultReader.reader("hits").reader("total").reader("value").readText().toString());
  }

  public int getSearchResults(JsonObject hits) {
    if (!enable7vFeatures) {
      return hits.get("total").getAsInt();
    }
    return hits.get("total").getAsJsonObject().get("value").getAsInt();
  }

  public byte[] getSearchBytes(ElasticConnectionPool.ElasticConnection connection, ElasticActions.Search<byte[]> search) {
      return connection.execute(search, elasticMajorVersion);
  }

  public <T> T executeMapping(ElasticConnectionPool.ElasticConnection connection, ElasticActions.ElasticAction2<T> putMapping) {
      return connection.execute(putMapping, elasticMajorVersion);
  }

  private static DateFormats.AbstractFormatterAndType[] getFormatterTypeArr(List<String> formats, Function<String, DateFormats.AbstractFormatterAndType> formatterFunction) {
    if (formats != null && !formats.isEmpty()) {
      return FluentIterable.from(formats).transform(formatterFunction).toArray(DateFormats.AbstractFormatterAndType.class);
    }
    return DateFormats.AbstractFormatterAndType.DEFAULT_FORMATTERS;
  }

  private static int readAsInt(String number) {
    return new LazilyParsedNumber(number).intValue();
  }
}
