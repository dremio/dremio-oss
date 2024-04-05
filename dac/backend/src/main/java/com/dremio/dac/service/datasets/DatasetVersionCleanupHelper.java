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
package com.dremio.dac.service.datasets;

import com.dremio.dac.util.DatasetsUtil;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.jobs.cleanup.ExternalCleaner;
import java.util.LinkedList;
import java.util.List;

/**
 * Helper class to create the DatasetVersion cleanup that deletes dataset versions associated to a
 * {@link JobAttempt} (that was deleted).
 */
public final class DatasetVersionCleanupHelper {

  private DatasetVersionCleanupHelper() {}

  public static ExternalCleaner datasetVersionCleaner(final LegacyKVStoreProvider kvStoreProvider) {
    return new ExternalCleaner() {
      @Override
      protected void doGo(JobAttempt jobAttempt) {
        final List<String> path = new LinkedList<>(jobAttempt.getInfo().getDatasetPathList());
        final String version = jobAttempt.getInfo().getDatasetVersion();
        if (DatasetsUtil.isTemporaryPath(path)) {
          DatasetVersionMutator.deleteDatasetVersion(kvStoreProvider, path, version);
        }
      }

      @Override
      public String getName() {
        return "DatasetVersionCleanup";
      }
    };
  }
}
