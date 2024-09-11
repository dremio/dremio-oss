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
package com.dremio.exec.planner.cost;

import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

public class RelMdMaxRowCount extends org.apache.calcite.rel.metadata.RelMdMaxRowCount {
  public static final RelMdMaxRowCount INSTANCE = new RelMdMaxRowCount();

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.MAX_ROW_COUNT.method, INSTANCE);

  public Double getMaxRowCount(ExchangePrel prel, RelMetadataQuery mq) {
    return mq.getMaxRowCount(prel.getInput());
  }

  public Double getMaxRowCount(TableFunctionPrel rel, RelMetadataQuery mq) {
    if (PrelUtil.getSettings(rel.getCluster())
        .getOptions()
        .getOption(PlannerSettings.USE_MAX_ROWCOUNT)) {
      if (rel.getTableMetadata().getDatasetConfig().getReadDefinition().getScanStats().getType()
          == ScanStatsType.EXACT_ROW_COUNT) {
        return rel.getTableMetadata()
            .getDatasetConfig()
            .getReadDefinition()
            .getScanStats()
            .getRecordCount()
            .doubleValue();
      }
    }
    return null;
  }
}
