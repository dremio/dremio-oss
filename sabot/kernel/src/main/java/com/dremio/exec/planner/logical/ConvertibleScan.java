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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import com.dremio.exec.store.ConversionContext;
import com.dremio.exec.store.StoragePlugin;
import com.google.common.base.Preconditions;

/**
 * A serializable logical scan that is transformable to a concrete scan.
 *
 * Used for serialization purposes.
 */
@Deprecated
public class ConvertibleScan extends TableScan {
  private final StoragePlugin plugin;
  private final ConversionContext context;

  public ConvertibleScan(final RelOptCluster cluster, final RelTraitSet traitSet, final RelOptTable table,
                         final StoragePlugin plugin, final ConversionContext context) {
    super(cluster, traitSet, table);
    this.plugin = Preconditions.checkNotNull(plugin, "plugin cannot be null");
    this.context = Preconditions.checkNotNull(context, "context cannot be null");
  }

  public RelNode convert() {
    return plugin.getRel(getCluster(), getTable(), context);
  }

}
