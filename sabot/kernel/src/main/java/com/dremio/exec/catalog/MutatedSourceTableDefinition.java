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
package com.dremio.exec.catalog;

import java.util.List;

import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.google.common.base.Function;

class MutatedSourceTableDefinition implements SourceTableDefinition {

  private final SourceTableDefinition delegate;
  private final Function<DatasetConfig, DatasetConfig> datasetMutator;


  public MutatedSourceTableDefinition(
      SourceTableDefinition delegate,
      Function<DatasetConfig, DatasetConfig> datasetMutator) {
    super();
    this.delegate = delegate;
    this.datasetMutator = datasetMutator;
  }

  @Override
  public NamespaceKey getName() {
    return delegate.getName();
  }

  @Override
  public DatasetConfig getDataset() throws Exception {
    return datasetMutator.apply(delegate.getDataset());
  }

  @Override
  public List<DatasetSplit> getSplits() throws Exception {
    return delegate.getSplits();
  }

  @Override
  public boolean isSaveable() {
    return delegate.isSaveable();
  }

  @Override
  public DatasetType getType() {
    return delegate.getType();
  }

}
