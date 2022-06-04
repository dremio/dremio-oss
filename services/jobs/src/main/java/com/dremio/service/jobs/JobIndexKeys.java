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
package com.dremio.service.jobs;

import static com.dremio.service.job.proto.QueryType.ACCELERATOR_CREATE;
import static com.dremio.service.job.proto.QueryType.ACCELERATOR_DROP;
import static com.dremio.service.job.proto.QueryType.ACCELERATOR_EXPLAIN;
import static com.dremio.service.job.proto.QueryType.FLIGHT;
import static com.dremio.service.job.proto.QueryType.INTERNAL_ICEBERG_METADATA_DROP;
import static com.dremio.service.job.proto.QueryType.JDBC;
import static com.dremio.service.job.proto.QueryType.METADATA_REFRESH;
import static com.dremio.service.job.proto.QueryType.ODBC;
import static com.dremio.service.job.proto.QueryType.PREPARE_INTERNAL;
import static com.dremio.service.job.proto.QueryType.REST;
import static com.dremio.service.job.proto.QueryType.UI_EXPORT;
import static com.dremio.service.job.proto.QueryType.UI_INITIAL_PREVIEW;
import static com.dremio.service.job.proto.QueryType.UI_INTERNAL_PREVIEW;
import static com.dremio.service.job.proto.QueryType.UI_INTERNAL_RUN;
import static com.dremio.service.job.proto.QueryType.UI_PREVIEW;
import static com.dremio.service.job.proto.QueryType.UI_RUN;
import static com.dremio.service.job.proto.QueryType.UNKNOWN;

import java.util.Map;

import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.indexed.FilterIndexMapping;
import com.dremio.datastore.indexed.IndexKey;
import com.google.common.collect.ImmutableMap;

/**
 * keys used to search/sort jobs
 */
public final class JobIndexKeys {
  private JobIndexKeys() {};

  public static final String UI = "UI";
  public static final String EXTERNAL = "EXTERNAL";
  public static final String ACCELERATION = "ACCELERATION";
  public static final String INTERNAL = "INTERNAL";
  public static final String DOWNLOAD = "DOWNLOAD";

  public static final SearchQuery UI_JOBS_FILTER = SearchQueryUtils.or(
      SearchQueryUtils.newTermQuery("QUERY_TYPE", UI_PREVIEW.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", UI_EXPORT.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", UI_RUN.toString()));

  public static final SearchQuery EXTERNAL_JOBS_FILTER = SearchQueryUtils.or(
      SearchQueryUtils.newTermQuery("QUERY_TYPE", ODBC.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", JDBC.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", REST.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", FLIGHT.toString()));

  public static final SearchQuery UI_EXTERNAL_JOBS_FILTER = SearchQueryUtils.or(
      UI_JOBS_FILTER,
      EXTERNAL_JOBS_FILTER);

  public static final SearchQuery UI_EXTERNAL_RUN_JOBS_FILTER = SearchQueryUtils.or(
      SearchQueryUtils.newTermQuery("QUERY_TYPE", UI_RUN.toString()),
      EXTERNAL_JOBS_FILTER);

  public static final SearchQuery ACCELERATION_JOBS_FILTER = SearchQueryUtils.or(
      SearchQueryUtils.newTermQuery("QUERY_TYPE", ACCELERATOR_CREATE.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", ACCELERATOR_EXPLAIN.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", ACCELERATOR_DROP.toString()));

  public static final SearchQuery INTERNAL_JOBS_FILTER = SearchQueryUtils.or(
      SearchQueryUtils.newTermQuery("QUERY_TYPE", UI_INTERNAL_PREVIEW.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", UI_INTERNAL_RUN.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", UI_INITIAL_PREVIEW.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", PREPARE_INTERNAL.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", METADATA_REFRESH.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", INTERNAL_ICEBERG_METADATA_DROP.toString()),
      SearchQueryUtils.newTermQuery("QUERY_TYPE", UNKNOWN.toString()));

  public static final SearchQuery DOWNLOAD_JOBS_FILTER = SearchQueryUtils.newTermQuery("QUERY_TYPE", UI_EXPORT.toString());

  private static final Map<String, SearchQuery> QUERY_TYPE_FILTERS = ImmutableMap.of(
      UI, UI_JOBS_FILTER,
      EXTERNAL, EXTERNAL_JOBS_FILTER,
      ACCELERATION, ACCELERATION_JOBS_FILTER,
      INTERNAL, INTERNAL_JOBS_FILTER,
      DOWNLOAD, DOWNLOAD_JOBS_FILTER
  );
  // Map short form to fields in search index. (This is done to keep url short)
  // Keep it short 2-3 letters.
  // Reserved keywords: gt, lt, le, ge, eq
  // Set sortable if jobs can sorted by that field. Make sure to add DocValues during indexing.
  public static final IndexKey JOBID = IndexKey.newBuilder("job", "JOBID", String.class)
    .setSortedValueType(SearchFieldSorting.FieldType.STRING)
    .setIncludeInSearchAllFields(true)
    .setStored(true)
    .build();
  public static final IndexKey USER = IndexKey.newBuilder("usr", "USER", String.class)
    .setSortedValueType(SearchFieldSorting.FieldType.STRING)
    .setIncludeInSearchAllFields(true)
    .build();
  public static final IndexKey SPACE = IndexKey.newBuilder("spc", "SPACE", String.class)
    .setIncludeInSearchAllFields(true)
    .build();
  public static final IndexKey DATASET = IndexKey.newBuilder("ds", "DATASET", String.class)
    .setSortedValueType(SearchFieldSorting.FieldType.STRING)
    .setIncludeInSearchAllFields(true)
    .build();
  public static final IndexKey DATASET_VERSION = IndexKey.newBuilder("dsv", "DATASET_VERSION", String.class)
    .setIncludeInSearchAllFields(true)
    .build();
  public static final IndexKey START_TIME = IndexKey.newBuilder("st", "START_TIME", Long.class)
    .setSortedValueType(SearchFieldSorting.FieldType.LONG)
    .build();
  public static final IndexKey END_TIME = IndexKey.newBuilder("et", "END_TIME", Long.class)
    .setSortedValueType(SearchFieldSorting.FieldType.LONG)
    .build();
  public static final IndexKey DURATION = IndexKey.newBuilder("dur", "DURATION", Long.class)
    .setSortedValueType(SearchFieldSorting.FieldType.LONG)
    .build();
  public static final IndexKey PARENT_DATASET = IndexKey.newBuilder("pds", "PARENT_DATASET", String.class)
    .setIncludeInSearchAllFields(true)
    .setCanContainMultipleValues(true)
    .build();
  public static final IndexKey JOB_STATE = IndexKey.newBuilder("jst", "JOB_STATE", String.class)
    .setIncludeInSearchAllFields(true)
    .build();
  public static final IndexKey SQL = IndexKey.newBuilder("sql", "SQL", String.class)
    .setIncludeInSearchAllFields(true)
    .build();
  public static final IndexKey QUERY_TYPE = IndexKey.newBuilder("qt", "QUERY_TYPE", String.class)
    .setReservedValues(QUERY_TYPE_FILTERS)
    .setCanContainMultipleValues(false)
    .build();
  public static final IndexKey QUEUE_NAME = IndexKey.newBuilder("qn", "QUEUE_NAME", String.class)
    .setSortedValueType(SearchFieldSorting.FieldType.STRING)
    .setIncludeInSearchAllFields(true)
    .build();
  // index all dataset this job accessed.
  public static final IndexKey ALL_DATASETS = IndexKey.newBuilder("ads", "ALL_DATASETS", String.class)
    .setIncludeInSearchAllFields(true)
    .setCanContainMultipleValues(true)
    .build();
  public static final FilterIndexMapping MAPPING = new FilterIndexMapping(JOBID, USER, SPACE, DATASET, DATASET_VERSION, START_TIME,
    END_TIME, DURATION, PARENT_DATASET, JOB_STATE, SQL, QUERY_TYPE, QUEUE_NAME, ALL_DATASETS);
}
