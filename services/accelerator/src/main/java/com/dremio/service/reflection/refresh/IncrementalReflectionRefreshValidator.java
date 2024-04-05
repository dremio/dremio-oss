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
package com.dremio.service.reflection.refresh;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.store.TableMetadata;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.Pointer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;

/**
 * Validates reflection to verify that we can do incremental refresh If not possible, validation
 * will return false, populate fullRefreshReason and do a full refresh. If possible, we will return
 * true, and potentially overwrite baseTableMetadata, baseTableSnapshotId and
 * lastRefreshBaseTableSnapshotId if needed
 */
public class IncrementalReflectionRefreshValidator {
  public boolean validate(
      final RelNode strippedPlan,
      final Pointer<String> fullRefreshReason,
      final Pointer<TableMetadata> baseTableMetadata,
      final Pointer<String> baseTableSnapshotId,
      final Pointer<String> baseTablePreviousRefreshSnapshotId,
      final UpdateId previousUpdateID) {
    Pointer<Boolean> hasUnsupportedJoin = new Pointer<>(false);
    strippedPlan.accept(
        new StatelessRelShuttleImpl() {
          @Override
          public RelNode visit(LogicalJoin logicalJoin) {
            hasUnsupportedJoin.value = true;
            fullRefreshReason.value =
                "Cannot do Incremental Refresh because the plan contains at least one LogicalJoin";
            return logicalJoin;
          }
        });
    return !hasUnsupportedJoin.value;
  }
}
