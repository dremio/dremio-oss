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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rel.metadata.RelMetadataCache;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;

public class DremioRelMetadataCache implements RelMetadataCache {
  public static final String MAX_METADATA_CALL_ERROR_MESSAGE =
      "Max Rel Metadata call count exceeded";
  private final AtomicLong putCallCount = new AtomicLong();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Map<RelNode, Map<Object, Object>> map = new HashMap<>();

  @Override
  public boolean clear(RelNode rel) {
    lock.writeLock().lock();
    try {
      return map.remove(rel) != null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void clear() {
    lock.writeLock().lock();
    try {
      map.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Object remove(RelNode relNode, Object args) {
    lock.writeLock().lock();
    try {
      Map<Object, Object> row = map.get(relNode);
      if (row == null) {
        return null;
      }
      return row.remove(args);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Object get(RelNode relNode, Object args) {

    lock.readLock().lock();
    try {
      Map<Object, Object> row = map.get(relNode);
      if (row == null) {
        return null;
      }
      return row.get(args);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Object put(RelNode relNode, Object args, Object value) {
    RelOptPlanner planner = relNode.getCluster().getPlanner();
    long pcc = putCallCount.incrementAndGet();
    lock.writeLock().lock();
    try {
      if (value != NullSentinel.ACTIVE || relNode instanceof RelSubset || relNode instanceof HepRelVertex) {
        Map<Object, Object> row = map.get(relNode);
        if (row == null) {
          //Only check when we see a new RelNode to make sure the overhead is minimized.
          if (planner instanceof AbstractRelOptPlanner) {
            PlannerSettings settings = PrelUtil.getPlannerSettings(planner);
            if (settings != null) {
              long maxCallCount = settings.maxMetadataCallCount();
              if (pcc > maxCallCount) {
                throw UserException.planError()
                  .message(MAX_METADATA_CALL_ERROR_MESSAGE).buildSilently();
              }
              ((AbstractRelOptPlanner) planner).checkCancel();
            }
          }
          row = new HashMap<>();
          map.put(relNode, row);
        }
        return row.put(args, value);
      } else {
        return null;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }
}
