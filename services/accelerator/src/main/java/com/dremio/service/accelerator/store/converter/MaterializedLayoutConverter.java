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
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.store.AccelerationIndexKeys;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

/**
 * Layout indexer
 */
public class MaterializedLayoutConverter implements KVStoreProvider.DocumentConverter<LayoutId, MaterializedLayout> {
  @Override
  public void convert(DocumentWriter writer, LayoutId key, MaterializedLayout record) {
    final String[] states = FluentIterable.from(AccelerationUtils.selfOrEmpty(record.getMaterializationList()))
        .transform(new Function<Materialization, String>() {
          @Nullable
          @Override
          public String apply(@Nullable final Materialization input) {
            return input.getState().toString();
          }
        })
        .toArray(String.class);

    writer.write(AccelerationIndexKeys.MATERIALIZATION_STATE, states);
  }
}
