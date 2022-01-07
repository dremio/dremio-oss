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

/**
 * Jobs Constants.
 */
public interface JobsConstant {
  String DOT_BACKSLASH = "\\.\"";
  String QUOTES = "\"";
  String __ACCELERATOR = "__accelerator";
  String REFLECTION = "REFLECTION";
  String PDS = "PHYSICAL_DATASET_SOURCE_FILE";
  String BYTES = " B";
  String KILOBYTES = " KB";
  String MEGABYTES = " MB";
  String GIGABYTES = " GB";
  String DEFAULT = "default";
  String LOCAL_BYTES_READ = "LOCAL_BYTES_READ";
  String TOTAL_BYTES_READ = "TOTAL_BYTES_READ";
  String ERROR_JOB_STATE_NOT_SET = "JobState must be set";
  String DEFAULT_DATASET_TYPE = "OTHERS";
  String VIRTUAL_DATASET = "VIRTUAL_DATASET";
  String EXTERNAL_QUERY = "external_query";
  String OTHERS = "OTHERS";
  String UNAVAILABLE = "Unavailable";
  String METADATA = "Catalog";
  String EMPTY_DATASET_FIELD= "";
  String RECORDS = " Records";
  String FORWARD_SLASH = " / ";
  String DATASET_GRAPH_ERROR = "Something went wrong while accessing one or more components of Dataset Graph. Please check your privileges.";
  String DOT= ".";
  String SCANNED_DATASET = "ScannedDataset";
  String ACCELERATOR = "ACCELERATOR";
  String DATASETGRAPH = "DatasetGraph";
  String ALGEBRAIC_REFLECTIONS = "AlgebraicReflections";
  String REFLECTIONS_MATCHED_OR_USED = "ReflectionsMatchedOrUsed";
  String AGGREGATION = "AGGREGATION";
  String  TMP_UNTITLED = "tmp.UNTITLED";
}
