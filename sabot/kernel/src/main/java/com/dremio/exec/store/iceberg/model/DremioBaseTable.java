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
package com.dremio.exec.store.iceberg.model;

import javax.annotation.Nullable;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.TableOperations;

import com.dremio.exec.store.iceberg.nessie.IcebergCommitOriginAwareTableOperations;

public class DremioBaseTable extends BaseTable {

  public DremioBaseTable(TableOperations ops, String name) {
    super(ops, name);
  }

  @Override
  public RewriteFiles newRewrite() {
    // we seem to use IcebergModel.getOptimizeCommitter instead but let's override to be safe
    tryNarrowCommitOrigin(null, IcebergCommitOrigin.OPTIMIZE_REWRITE_DATA_TABLE);
    return super.newRewrite();
  }

  @Override
  public RewriteManifests rewriteManifests() {
    tryNarrowCommitOrigin(null, IcebergCommitOrigin.OPTIMIZE_REWRITE_MANIFESTS_TABLE);
    return super.rewriteManifests();
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    tryNarrowCommitOrigin(null, IcebergCommitOrigin.EXPIRE_SNAPSHOTS);
    return super.expireSnapshots();
  }

  private void tryNarrowCommitOrigin(@Nullable IcebergCommitOrigin oldOrigin, IcebergCommitOrigin newOrigin) {
    TableOperations ops = operations();
    if (ops instanceof IcebergCommitOriginAwareTableOperations) {
      ((IcebergCommitOriginAwareTableOperations) ops).tryNarrowCommitOrigin(oldOrigin, newOrigin);
    }
  }
}
