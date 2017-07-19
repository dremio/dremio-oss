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
package com.dremio.service.namespace;

import java.util.List;

import com.dremio.common.exceptions.UserException;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;

/**
 * A wrapper for DatasetAccessor that creates friendly user exceptions for any failures.
 */
public class QuietAccessor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QuietAccessor.class);

  private final SourceTableDefinition delegate;

  public QuietAccessor(SourceTableDefinition delegate) {
    this.delegate = delegate;
  }

  public NamespaceKey getName(){
    return delegate.getName();
  }

  public DatasetConfig getDataset(){
    try{
      return delegate.getDataset();
    }catch(Exception e){
      throw UserException.dataReadError(e).message("Failure while attempting to retrieve metadata information for table %s.", getName()).build(logger);
    }
  }

  public List<DatasetSplit> getSplits() {
    try{
      return delegate.getSplits();
    }catch(Exception e){
      throw UserException.dataReadError(e).message("Failure while attempting to retrieve parallelization information for table %s.", getName()).build(logger);
    }
  }

}
