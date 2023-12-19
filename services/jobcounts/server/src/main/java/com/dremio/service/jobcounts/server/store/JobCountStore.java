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
package com.dremio.service.jobcounts.server.store;

import java.util.List;

import com.dremio.service.Service;
import com.dremio.service.jobcounts.JobCountType;
import com.dremio.service.jobcounts.JobCountUpdate;

/**
 * Jobs count store interface.
 */
public interface JobCountStore extends Service {

  int getCount(String id, JobCountType type, int jobCountAgeInDays);

  List<Integer> getCounts(List<String> ids, JobCountType type, int jobCountAgeInDays);

  default void updateCount(String id, JobCountType type) {
    updateCount(id, type, System.currentTimeMillis());
  }

  void updateCount(String id, JobCountType type, long currTimeMillis);

  void bulkUpdateCount(List<JobCountUpdate> countUpdates);

  void deleteCount(String id);

  void bulkDeleteCount(List<String> ids);
}
