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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.Reference;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.model.IcebergBaseCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;

public class IcebergNessieVersionedCommand extends IcebergBaseCommand {
  private final IcebergNessieVersionedTableIdentifier nessieTableIdentifier;
  private final NessieApiV1 nessieClient;
  private final OperatorStats operatorStats;

  public IcebergNessieVersionedCommand(IcebergTableIdentifier tableIdentifier,
                                       Configuration configuration,
                                       NessieApiV1 nessieClient,
                                       FileSystem fs,
                                       OperatorContext context, List<String> dataset) {
    super(configuration, ((IcebergNessieVersionedTableIdentifier) tableIdentifier).getTableFolder(), fs, context, dataset);
    nessieTableIdentifier = ((IcebergNessieVersionedTableIdentifier) tableIdentifier);
    this.nessieClient = nessieClient;
    this.operatorStats = context == null ? null : context.getStats();
  }

  @Override
  public TableOperations getTableOperations() {

    TableIdentifier tid = nessieTableIdentifier.getTableIdentifier();
//    ContentsKey contentsKey = ContentsKey.of(tid.namespace().toString(), tid.name());
    ContentsKey contentsKey = ContentsKey.of(Collections.singletonList(nessieTableIdentifier.getNessieKey()));
    //Construct the version Reference
    VersionContext versionContext = nessieTableIdentifier.getVersionContext();
    try {
      return new IcebergNessieVersionedTableOperations(
        operatorStats,
        new DremioFileIO(fs, context, dataset, null, null, configuration),
        nessieClient,
        contentsKey,
        getNessieReference(versionContext));
    } catch (NessieNotFoundException e) {
      throw UserException.dataReadError(e).buildSilently();
    }
  }

  private Reference getNessieReference(VersionContext versionContext) throws NessieNotFoundException {
    // todo: cache references
    if (versionContext.getBranchOrTagName() != null) {
      return nessieClient.getReference().refName(versionContext.getBranchOrTagName()).get();
    }
    return nessieClient.getDefaultBranch();
  }

  public void deleteRootPointerStoreKey() {
    try {
      IcebergNessieVersionedTableOperations tableOps = new IcebergNessieVersionedTableOperations(
        operatorStats,
        new DremioFileIO(fs, context, dataset, null, null, configuration),
        nessieClient,
        ContentsKey.of(Collections.singletonList(nessieTableIdentifier.getNessieKey())),
        getNessieReference(nessieTableIdentifier.getVersionContext()));
      tableOps.deleteKey();
    } catch (NessieConflictException | NessieNotFoundException e) {
      throw UserException.dataReadError(e).buildSilently();
    }
  }
}
