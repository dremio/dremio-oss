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
package com.dremio.dac.cmd.upgrade;

import java.util.Map.Entry;

import com.dremio.common.Version;
import com.dremio.dac.cmd.AdminLogger;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.jobs.LocalJobsService.JobsStoreCreator;
import com.google.common.collect.ImmutableList;

/**
 * Remove the schema stored in Arrow file footers of job results in KV store (See DX-12627) to reduce the KV store
 * size
 */
public class MinimizeJobResultsMetadata extends UpgradeTask implements LegacyUpgradeTask {


  //DO NOT MODIFY
  static final String taskUUID = "c33400d9-fa65-47e2-b99a-5c3db12d8f84";

  public MinimizeJobResultsMetadata() {
    super("Delete schema stored in arrow footers of job results in KV Store", ImmutableList.of(DeleteHive121BasedInputSplits.taskUUID));
  }

  @Override
  public Version getMaxVersion() {
    return VERSION_212;
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final LegacyIndexedStore<JobId, JobResult> store = context.getKVStoreProvider().getStore(JobsStoreCreator.class);

    AdminLogger.log("  Minimizing job results metadata");
    try {
      for (Entry<JobId, JobResult> entry : store.find()) {
        final JobResult jobResult = entry.getValue();
        if (jobResult == null || jobResult.getAttemptsList() == null) {
          continue;
        }

        for (JobAttempt attempt : jobResult.getAttemptsList()) {
          if (attempt == null || attempt.getInfo() == null || attempt.getInfo().getResultMetadataList() == null) {
            continue;
          }
          for (ArrowFileMetadata metadata : attempt.getInfo().getResultMetadataList()) {
            if (metadata.getFooter() != null) {
              metadata.getFooter().setFieldList(null);
            }
          }
        }
        store.put(entry.getKey(), jobResult);
      }
    } catch (Exception e) {
      throw new RuntimeException("  Failed to minimize job results metadata", e);
    }
  }

  @Override
  public String toString() {
    return String.format("'%s' up to %s)", getDescription(), getMaxVersion());
  }
}
