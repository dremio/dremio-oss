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

import com.dremio.datastore.indexed.IndexKey;
import com.google.common.collect.ImmutableMap;

/**
 * Provides methods for LocalJobsService to convert the SearchQuery in Request to the appropriate
 * SearchQuery according to what columns are indexed. "FIELDS" map maps the column names to
 * appropriate JobIndexKey.
 */
public class JobSearchUtils {
  public static final ImmutableMap<String, IndexKey> FIELDS =
      ImmutableMap.<String, IndexKey>builder()
          .put("Job_Id".toLowerCase(), JobIndexKeys.JOBID)
          .put("User_Name".toLowerCase(), JobIndexKeys.USER)
          .put("Status".toLowerCase(), JobIndexKeys.JOB_STATE)
          .put("Query_Type".toLowerCase(), JobIndexKeys.QUERY_TYPE)
          .put("Submitted_Epoch_Millis".toLowerCase(), JobIndexKeys.START_TIME)
          .build();
}
