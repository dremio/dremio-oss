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
package com.dremio.exec.ops;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.CatalogService;
import com.dremio.sabot.exec.context.FunctionContext;

public interface OptimizerRulesContext extends FunctionContext {
  /**
   * Method returns the function registry
   * @return FunctionImplementationRegistry
   */
  public FunctionImplementationRegistry getFunctionRegistry();

  /**
   * Method returns the allocator
   * @return BufferAllocator
   */
  public BufferAllocator getAllocator();

  /**
   * Method returns the planner options
   * @return PlannerSettings
   */
  public PlannerSettings getPlannerSettings();

  // TODO(DX-43968): Rework to not expose catalog service; optimization is contextualized to Catalog
  public CatalogService getCatalogService();
}
