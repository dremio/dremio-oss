/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.fragment;

import java.util.Collection;

import com.dremio.exec.physical.PhysicalOperatorSetupException;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;

/**
 * Generic interface to provide different parallelization strategies for MajorFragments.
 */
public interface FragmentParallelizer {
  /**
   * Parallelize the given fragment.
   *
   * @param fragment
   * @param parameters
   * @param activeEndpoints
   * @throws PhysicalOperatorSetupException
   */
  void parallelizeFragment(final Wrapper fragment, final ParallelizationParameters parameters,
      final Collection<NodeEndpoint> activeEndpoints) throws PhysicalOperatorSetupException;
}
