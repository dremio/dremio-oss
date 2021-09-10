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
package com.dremio.exec.store.iceberg.hadoop;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableOperations;

import com.dremio.exec.store.iceberg.model.IcebergBaseCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * This class interacts with Iceberg libraries and helps mutation commands
 * to manage a transaction on a table. It requires configuration instance from
 * file system plugins, so that it can read and write from file system sources
 */
class IcebergHadoopCommand extends IcebergBaseCommand {

  public IcebergHadoopCommand(IcebergTableIdentifier tableIdentifier, Configuration configuration,
                              FileSystem fs, OperatorContext context, List<String> dataset) {
    super(configuration, ((IcebergHadoopTableIdentifier)tableIdentifier).getTableFolder(), fs, context, dataset);
  }

  @Override
  public TableOperations getTableOperations() {
    return new IcebergHadoopTableOperations(fsPath, configuration, fs, context, dataset);
  }

  @Override
  public void deleteRootPointerStoreKey() {
  // TODO if required
  }
}
