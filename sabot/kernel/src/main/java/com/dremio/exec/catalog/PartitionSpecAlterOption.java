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
package com.dremio.exec.catalog;

import com.dremio.exec.planner.sql.PartitionTransform;
import com.dremio.exec.planner.sql.parser.SqlAlterTablePartitionColumns;

public class PartitionSpecAlterOption implements AlterTableOption {
  PartitionTransform partitionTransform;
  SqlAlterTablePartitionColumns.Mode mode;

  public PartitionSpecAlterOption(
      PartitionTransform partitionTransform, SqlAlterTablePartitionColumns.Mode mode) {
    this.partitionTransform = partitionTransform;
    this.mode = mode;
  }

  public PartitionTransform getPartitionTransform() {
    return partitionTransform;
  }

  public void setPartitionTransform(PartitionTransform partitionTransform) {
    this.partitionTransform = partitionTransform;
  }

  public SqlAlterTablePartitionColumns.Mode getMode() {
    return mode;
  }

  public void setMode(SqlAlterTablePartitionColumns.Mode mode) {
    this.mode = mode;
  }

  @Override
  public Type getType() {
    return Type.PARTITION_SPEC_UPDATE;
  }
}
