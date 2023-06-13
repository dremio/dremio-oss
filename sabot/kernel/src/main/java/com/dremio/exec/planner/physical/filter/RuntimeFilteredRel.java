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

package com.dremio.exec.planner.physical.filter;

import java.util.List;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.store.TableMetadata;

public interface RuntimeFilteredRel extends RelNode {
  List<Info> getRuntimeFilters();
  void addRuntimeFilter(Info filterInfo);
  TableMetadata getTableMetadata();

  enum ColumnType {
    PARTITION {
      @Override
      protected String alias() {
        return "p";
      }
    }, RANDOM{
      @Override
      protected String alias() {
        return "r";
      }
    };

    protected abstract String alias();
  }

  class Info {
    private final RuntimeFilterId runtimeFilterId;
    private final ColumnType columnType;
    private final String filteredColumnName;
    private final String filteringColumnName;

    public Info(RuntimeFilterId runtimeFilterId,
        ColumnType columnType,
        String filteredColumnName,
        String filteringColumnName) {
      this.runtimeFilterId = runtimeFilterId;
      this.columnType = columnType;
      this.filteredColumnName = filteredColumnName;
      this.filteringColumnName = filteringColumnName;
    }

    public RuntimeFilterId getRuntimeFilterId() {
      return runtimeFilterId;
    }

    public ColumnType getColumnType() {
      return columnType;
    }

    public String getFilteredColumnName() {
      return filteredColumnName;
    }
    public String getFilteringColumnName() {
      return filteringColumnName;
    }

    @Override
    public String toString() {
      //used in rel explain
      return columnType.alias() + "_" + runtimeFilterId + ":" +
        filteringColumnName + "->" + filteredColumnName;
    }
  }


}
