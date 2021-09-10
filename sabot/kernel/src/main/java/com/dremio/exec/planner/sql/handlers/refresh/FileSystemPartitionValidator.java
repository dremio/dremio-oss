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
package com.dremio.exec.planner.sql.handlers.refresh;

import java.util.concurrent.atomic.AtomicInteger;

import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.google.common.base.Preconditions;

public class FileSystemPartitionValidator extends RefreshDatasetValidator {

  public FileSystemPartitionValidator(UnlimitedSplitsMetadataProvider metadataProvider) {
    super(metadataProvider);
  }

  @Override
  public void validate(SqlRefreshDataset sqlNode) {
    super.validate(sqlNode);
    AtomicInteger i = new AtomicInteger();

    partitionValues.stream().forEach(x -> {
      Preconditions.checkArgument(x.getColumn().matches("dir" + i), "Input error. Expected partition dir" + i);
      i.getAndIncrement();
    });
  }
}
