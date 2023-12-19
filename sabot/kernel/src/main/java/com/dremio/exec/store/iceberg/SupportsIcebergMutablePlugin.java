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
package com.dremio.exec.store.iceberg;


import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.FileIO;

import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.sabot.exec.context.OperatorContext;

public interface SupportsIcebergMutablePlugin extends MutablePlugin, SupportsIcebergRootPointer {
  /**
   *
   * @param tableProps Iceberg table props
   * @param userName userName of current user
   * @param context  Operator Context
   * @param fileIO FileIO instance for creating the Iceberg Model
   * @return IcebergModel which is used for performing Iceberg operations
   */
  IcebergModel getIcebergModel(IcebergTableProps tableProps, String userName,
                               OperatorContext context, FileIO fileIO);

  /**
   *
   * @param tableProps
   * @return root folder path for table
   * For versioned plugin, root path where table will be created
   */
  default String getTableLocation(IcebergTableProps tableProps) {
    return tableProps.getTableLocation();
  }

  /**
   * @return A copy of the configuration to use for the plugin.
   */
  @Override
  default Configuration getFsConfCopy() {
    throw new UnsupportedOperationException();
  }
}
