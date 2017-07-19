/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner.logical;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;

import com.dremio.common.JSONOptions;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.calcite.logical.OldScanCrel;
import com.dremio.exec.physical.base.OldAbstractGroupScan;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.common.OldScanRelBase;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.util.ImpersonationUtil;

public abstract class TableBase implements TranslatableTable {

  private final String storageEngineName;
  private final StoragePluginConfig storageEngineConfig;
  private final TableType tableType;
  private final Object selection;
  private final StoragePlugin plugin;
  private final String userName;
  protected final List<String> tableSchemaPath;

  private GroupScan scan;

  /**
   * Creates a TableBase instance for a {@code TableType#TABLE}
   * @param storageEngineName StorageEngine name.
   * @param plugin Reference to StoragePlugin.
   * @param userName Whom to impersonate while reading the contents of the table.
   * @param selection Table contents (type and contents depend on type of StoragePlugin).
   */
  public TableBase(String storageEngineName, StoragePlugin plugin, String userName, Object selection, List<String> tableSchemaPath) {
    this(storageEngineName, plugin, TableType.TABLE, userName, selection, tableSchemaPath);
  }

  /**
   * Creates a TableBase instance
   * @param storageEngineName StorageEngine name.
   * @param plugin Reference to StoragePlugin.
   * @param tableType the table type
   * @param userName Whom to impersonate while reading the contents of the table.
   * @param selection Table contents (type and contents depend on type of StoragePlugin).
   */
  public TableBase(String storageEngineName, StoragePlugin plugin, TableType tableType, String userName, Object selection, List<String> tableSchemaPath) {
    this.selection = selection;
    this.plugin = plugin;
    this.tableType = tableType;

    this.storageEngineConfig = plugin.getConfig();
    this.storageEngineName = storageEngineName;
    this.userName = userName;
    this.tableSchemaPath = tableSchemaPath;
  }

  /**
   * TODO: Same purpose as other constructor except the impersonation user is the user who is running the SabotNode
   * process. Once we add impersonation to non-FileSystem storage plugins such as Hive, HBase etc,
   * we can remove this constructor.
   */
  public TableBase(String storageEngineName, StoragePlugin plugin, Object selection, List<String> tableSchemaPath) {
    this(storageEngineName, plugin, ImpersonationUtil.getProcessUserName(), selection, tableSchemaPath);
  }

  public GroupScan getGroupScan() throws IOException{
    if (scan == null) {
      this.scan = plugin.getPhysicalScan(userName, new JSONOptions(selection), tableSchemaPath, OldAbstractGroupScan.ALL_COLUMNS);
    }
    return scan;
  }

  public StoragePluginConfig getStorageEngineConfig() {
    return storageEngineConfig;
  }

  public StoragePlugin getPlugin() {
    return plugin;
  }

  public Object getSelection() {
    return selection;
  }

  public String getStorageEngineName() {
    return storageEngineName;
  }

  public String getUserName() {
    return userName;
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable table) {
    try {
      return new OldScanCrel(
        context.getCluster(),
        table,
        context.getCluster().traitSetOf(Convention.NONE),
        getRowType(context.getCluster().getTypeFactory()),
        getGroupScan(),
        null,
        OldScanRelBase.DEFAULT_ROW_COUNT_DISCOUNT);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TableType getJdbcTableType() {
    return tableType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((selection == null) ? 0 : selection.hashCode());
    result = prime * result + ((storageEngineConfig == null) ? 0 : storageEngineConfig.hashCode());
    result = prime * result + ((storageEngineName == null) ? 0 : storageEngineName.hashCode());
    result = prime * result + ((userName == null) ? 0 : userName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TableBase other = (TableBase) obj;
    if (selection == null) {
      if (other.selection != null) {
        return false;
      }
    } else if (!selection.equals(other.selection)) {
      return false;
    }
    if (storageEngineConfig == null) {
      if (other.storageEngineConfig != null) {
        return false;
      }
    } else if (!storageEngineConfig.equals(other.storageEngineConfig)) {
      return false;
    }
    if (storageEngineName == null) {
      if (other.storageEngineName != null) {
        return false;
      }
    } else if (!storageEngineName.equals(other.storageEngineName)) {
      return false;
    }
    if (userName == null) {
      if (other.userName != null) {
        return false;
      }
    } else if (!userName.equals(other.userName)) {
      return false;
    }
    return true;
  }

}
