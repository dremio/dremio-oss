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
package com.dremio.exec.store.iceberg.nessie;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.model.IcebergBaseModel;
import com.dremio.exec.store.iceberg.model.IcebergCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.plugins.NessieClient;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;

public class IcebergNessieVersionedModel extends IcebergBaseModel {
  private final List<String> tableKey;
  private final NessieClient nessieClient;
  private final ResolvedVersionContext version;
  private final  MutablePlugin plugin;

  public IcebergNessieVersionedModel(List<String> tableKey,
                                     Configuration fsConf,
                                     final NessieClient nessieClient,
                                     OperatorContext context, // Used to create DremioInputFile (valid only for insert/ctas)
                                     ResolvedVersionContext version,
                                     MutablePlugin plugin) {
    super(null, fsConf, null, context, null, plugin);

    this.tableKey = tableKey;
    this.nessieClient = nessieClient;

    Preconditions.checkNotNull(version);
    this.version = version;
    this.plugin = plugin;
  }

  protected IcebergCommand getIcebergCommand(IcebergTableIdentifier tableIdentifier) {
    String jobId = null;

    //context is only available for executors
    if (context != null) {
      jobId = QueryIdHelper.getQueryId(context.getFragmentHandle().getQueryId());
    }

    IcebergNessieVersionedTableOperations tableOperations = new IcebergNessieVersionedTableOperations(
      context == null ? null : context.getStats(),
      new DremioFileIO(fs, context, null, null, null, configuration, plugin),
      nessieClient,
      ((IcebergNessieVersionedTableIdentifier) tableIdentifier), jobId);

    return new IcebergNessieVersionedCommand(tableIdentifier, configuration,  fs, tableOperations, plugin);
  }

  @Override
  public IcebergTableIdentifier getTableIdentifier(String rootFolder) {
    return new IcebergNessieVersionedTableIdentifier(tableKey, rootFolder, version);
  }
}
