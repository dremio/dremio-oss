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
package com.dremio.exec.store.sys;

import java.util.Iterator;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordDataType;
import com.dremio.exec.store.pojo.PojoDataType;
import com.dremio.exec.store.sys.OptionIterator.OptionValueWrapper;
import com.dremio.exec.store.sys.accel.AccelerationInfo;
import com.dremio.exec.store.sys.accel.LayoutInfo;
import com.dremio.exec.store.sys.accel.MaterializationInfo;
import com.dremio.exec.work.WorkStats.FragmentInfo;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * An enumeration of all tables in Dremio's system ("sys") schema.
 * <p>
 *   OPTION, NODES and VERSION are local tables available on every SabotNode.
 *   MEMORY and THREADS are distributed tables with one record on every
 *   SabotNode.
 * </p>
 */
public enum SystemTable {

  OPTION("options", false, OptionValueWrapper.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext dbContext, final OperatorContext context) {
      return new OptionIterator(dbContext, context, OptionIterator.Mode.SYS_SESS);
    }
  },

  BOOT("boot", false, OptionValueWrapper.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext dbContext, final OperatorContext context) {
      return new OptionIterator(dbContext, context, OptionIterator.Mode.BOOT);
    }
  },

  NODES("nodes", true, NodeIterator.NodeInstance.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext dbContext, final OperatorContext context) {
      return new NodeIterator(dbContext, context);
    }
  },

  // TODO - should be possibly make this a distributed table so that we can figure out if
  // users have inconsistent versions installed across their cluster?
  VERSION("version", false, VersionIterator.VersionInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext dbContext, final OperatorContext context) {
      return new VersionIterator();
    }
  },

  /*

   TODO - DRILL-4258: fill in these system tables
    cpu: SabotNode, # Cores, CPU consumption (with different windows?)
    queries: Foreman, QueryId, User, SQL, Start Time, rows processed, query plan, # nodes involved, number of running fragments, memory consumed
    fragments: SabotNode, queryid, major fragmentid, minorfragmentid, coordinate, memory usage, rows processed, start time
    threads: name, priority, state, id, thread-level cpu stats
    threadtraces: threads, stack trace
    connections: client, server, type, establishedDate, messagesSent, bytesSent
   */

  MEMORY("memory", true, MemoryIterator.MemoryInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext dbContext, final OperatorContext context) {
      return new MemoryIterator(dbContext, context);
    }
  },

  THREADS("threads", true, ThreadsIterator.ThreadSummary.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext dbContext, final OperatorContext context) {
      return new ThreadsIterator(dbContext, context);
    }
  },

  QUERIES("queries", true, QueryIterator.QueryInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext dbContext, final OperatorContext context) {
      return new QueryIterator(dbContext, context);
    }
  },

  FRAGMENTS("fragments", true, FragmentInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext dbContext, final OperatorContext context) {
      return new FragmentIterator(dbContext, context);
    }
  },

  ACCELERATIONS("accelerations", false, AccelerationInfo.class){
    @Override
    public Iterator<Object> getIterator(final SabotContext dbContext, final OperatorContext context) {
      return (Iterator<Object>) (Object) dbContext.getAccelerationListManager().getAccelerations().iterator();
    }
  },

  LAYOUTS("layouts", false, LayoutInfo.class){
    @Override
    public Iterator<Object> getIterator(final SabotContext dbContext, final OperatorContext context) {
      return (Iterator<Object>) (Object) dbContext.getAccelerationListManager().getLayouts().iterator();
    }
  },

  MATERIALIZATIONS("materializations", false, MaterializationInfo.class){
    @Override
    public Iterator<Object> getIterator(final SabotContext dbContext, final OperatorContext context) {
      return (Iterator<Object>) (Object) dbContext.getAccelerationListManager().getMaterializations().iterator();
    }
  }



  ;



//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemTable.class);

  private final String tableName;
  private final boolean distributed;
  private final Class<?> pojoClass;

  SystemTable(final String tableName, final boolean distributed, final Class<?> pojoClass) {
    this.tableName = tableName;
    this.distributed = distributed;
    this.pojoClass = pojoClass;
  }

  public Iterator<Object> getIterator(final SabotContext dbContext, final OperatorContext context) {
    throw new UnsupportedOperationException(tableName + " must override this method.");
  }

  public String getTableName() {
    return tableName;
  }

  public boolean isDistributed() {
    return distributed;
  }

  public Class<?> getPojoClass() {
    return pojoClass;
  }

  public BatchSchema getSchema() {
    RecordDataType dataType = new PojoDataType(pojoClass);
    RelDataType type = dataType.getRowType(new JavaTypeFactoryImpl());
    return BatchSchema.fromCalciteRowType(type);
  }

}
