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
package com.dremio.exec.physical.base;

/**
 * An interface which supports storing a record stream. In contrast to the logical layer, in the physical/execution
 * layers, a Store node is actually an outputting node (rather than a root node) that provides returns one or more
 * records regarding the completion of the query.
 */
public interface Store extends PhysicalOperator {

  /**
   * Get the child of this store operator as this will be needed for parallelization materialization purposes.
   * @return
   */
  public abstract PhysicalOperator getChild();
}