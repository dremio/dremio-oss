/*
 * Copyright (C) 2017-2018 Dremio Corporation
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


import static com.dremio.service.accelerator.AccelerationUtils.makePathString;

import java.util.List;

import javax.annotation.Nullable;

import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.KVStoreProvider.DocumentWriter;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.store.AccelerationIndexKeys;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

/**
 * Converts acceleration into an indexable lucene document.
 */
public class AccelerationConverter implements KVStoreProvider.DocumentConverter<AccelerationId, Acceleration> {

  private static final JobId EMPTY_JOB_ID = new JobId();

  @Override
  public void convert(DocumentWriter writer, AccelerationId key, Acceleration acceleration) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(key.getId());

    writer.write(AccelerationIndexKeys.ACCELERATION_ID, key.getId());
    writer.write(AccelerationIndexKeys.ACCELERATION_STATE, acceleration.getState().name());

    final String[] ids = getActiveLayoutIds(acceleration);
    writer.write(AccelerationIndexKeys.LAYOUT_ID, ids);

    final String[] types = getActiveLayoutTypes(acceleration);
    writer.write(AccelerationIndexKeys.LAYOUT_TYPE, types);

    final DatasetConfig dataset = acceleration.getContext().getDataset();
    final String path = makePathString(dataset.getFullPathList());
    writer.write(AccelerationIndexKeys.DATASET_PATH, path);

    final DatasetType type = dataset.getType();
    if (type == DatasetType.VIRTUAL_DATASET) {
      final String[] parents = FluentIterable
          .from(AccelerationUtils.selfOrEmpty(dataset.getVirtualDataset().getParentsList()))
          .transform(new Function<ParentDataset, String>() {
            @Nullable
            @Override
            public String apply(@Nullable final ParentDataset parent) {
              return makePathString(parent.getDatasetPathList());
            }
          })
          .toArray(String.class);

      writer.write(AccelerationIndexKeys.PARENT_DATASET_PATH, parents);
    }

    final JobId jobId = Optional.fromNullable(acceleration.getContext().getJobId()).or(EMPTY_JOB_ID);
    writer.write(AccelerationIndexKeys.JOB_ID, jobId.getId());

    final long footprint = computeAggregateFootprint(acceleration);
    writer.write(AccelerationIndexKeys.FOOTPRINT, footprint);
  }

  private static String[] getActiveLayoutIds(final Acceleration acceleration) {
    return FluentIterable.from(AccelerationUtils.allActiveLayouts(acceleration))
        .transform(new Function<Layout, String>() {
          @Nullable
          @Override
          public String apply(@Nullable final Layout input) {
            return input.getId().getId();
          }
        })
        .toArray(String.class);
  }

  private static String[] getActiveLayoutTypes(final Acceleration acceleration) {
    final List<String> types = Lists.newArrayList();
    if (acceleration.getRawLayouts().getEnabled()) {
      types.add(LayoutType.RAW.name());
    }
    if (acceleration.getAggregationLayouts().getEnabled()) {
      types.add(LayoutType.AGGREGATION.name());
    }
    final String[] typesArr = new String[types.size()];
    types.toArray(typesArr);
    return typesArr;
  }

  private static long computeAggregateFootprint(final Acceleration acceleration) {
    // TODO: implement this
    return 0;
  }
}
