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
package com.dremio.exec.work;

import java.util.List;

import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.google.common.base.Preconditions;

public class QueryWorkUnit {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryWorkUnit.class);
  private final List<PlanFragment> fragments;

  public QueryWorkUnit(final List<PlanFragment> fragments) {
    Preconditions.checkNotNull(fragments);

    this.fragments = fragments;
  }

  public List<PlanFragment> getFragments() {
    return fragments;
  }
}
