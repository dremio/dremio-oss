/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.accelerator.store.converter;


import javax.annotation.Nullable;

import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.KVStoreProvider.DocumentWriter;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.AccelerationEntry;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.DatasetConfigDescriptor;
import com.dremio.service.accelerator.proto.ParentDatasetDescriptor;
import com.dremio.service.accelerator.store.AccelerationIndexKeys;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;

/**
 * Converts acceleration into an indexable lucene document.
 */
public class AccelerationEntryConverter implements KVStoreProvider.DocumentConverter<AccelerationId, AccelerationEntry> {

  private static final JobId EMPTY_JOB_ID = new JobId();

  @Override
  public void convert(DocumentWriter writer, AccelerationId key, AccelerationEntry entry) {
    final long totalRequests = AccelerationUtils.selfOrEmpty(entry.getRequestList()).size();
    writer.write(AccelerationIndexKeys.TOTAL_REQUESTS, totalRequests);

    final DatasetConfigDescriptor dataset = entry.getDescriptor().getContext().getDataset();

    final DatasetType type = dataset.getType();
    if (type == DatasetType.VIRTUAL_DATASET) {
      final String[] parents = FluentIterable
          .from(AccelerationUtils.selfOrEmpty(dataset.getVirtualDataset().getParentList()))
          .transform(new Function<ParentDatasetDescriptor, String>() {
            @Nullable
            @Override
            public String apply(@Nullable final ParentDatasetDescriptor parent) {
              return AccelerationUtils.makePathString(parent.getPathList());
            }
          })
          .toArray(String.class);

      writer.write(AccelerationIndexKeys.PARENT_DATASET_PATH, parents);
    }

    final JobId jobId = Optional.fromNullable(entry.getDescriptor().getContext().getJobId()).or(EMPTY_JOB_ID);
    writer.write(AccelerationIndexKeys.JOB_ID, jobId.getId());
  }

}
