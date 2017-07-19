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
package com.dremio.exec.planner.sql.handlers;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.calcite.logical.SampleCrel;
import com.dremio.exec.planner.StatelessRelShuttleImpl;

class InjectSampleAndJdbcLogical extends StatelessRelShuttleImpl {

  private final boolean addSample;
  private final boolean addJdbcLogical;


  // If leaf limits are enabled, add it now during calcite logical planning (previously we were adding this at the
  // end of the convertToDrel, which would be too late for jdbc plugin/conventions.  The storage plugin rules would
  // have fired by the end of convertToDrel.  One thing to note, currently, we do not push down limit to jdbc (see
  // calcite JdbcRules.JdbcSortRule (offsets/fetchs are not pushed down).
  public InjectSampleAndJdbcLogical(boolean addSample, boolean addJdbcLogical) {
    this.addSample = addSample;
    this.addJdbcLogical = addJdbcLogical;
  }

  @Override
  public RelNode visit(TableScan scan) {
    RelNode toReturn = scan;
    if (addJdbcLogical && scan.getConvention() instanceof JdbcConventionIndicator) {
      toReturn = new JdbcCrel(scan.getCluster(), scan.getTraitSet().plus(Convention.NONE), scan);
    }
    if (addSample) {
      return SampleCrel.create(toReturn);
    }
    return toReturn;
  }
}
