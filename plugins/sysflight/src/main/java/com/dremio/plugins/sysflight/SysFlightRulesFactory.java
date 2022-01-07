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
package com.dremio.plugins.sysflight;

import java.util.Set;

import org.apache.calcite.plan.RelOptRule;

import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.google.common.collect.ImmutableSet;

/**
 * Rules factory for sys-flight plugin
 */
public class SysFlightRulesFactory extends StoragePluginTypeRulesFactory {

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, SourceType pluginType) {
    switch(phase) {
    case LOGICAL:
      return ImmutableSet.<RelOptRule>of(new SysFlightScanDrule(pluginType));

    case PHYSICAL:
      return ImmutableSet.of(
        new SysFlightScanPrule(optimizerContext.getCatalogService().getSource(CatalogServiceImpl.SYSTEM_TABLE_SOURCE_NAME)),
        SysFlightPushFilterIntoScan.IS_FILTER_ON_PROJECT,
        SysFlightPushFilterIntoScan.IS_FILTER_ON_SCAN);

    default:
      return ImmutableSet.of();
    }
  }

}
