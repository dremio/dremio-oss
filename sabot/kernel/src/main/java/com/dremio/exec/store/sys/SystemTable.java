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
package com.dremio.exec.store.sys;

import java.util.Iterator;
import java.util.stream.StreamSupport;

import org.apache.calcite.rel.type.RelDataType;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordDataType;
import com.dremio.exec.store.pojo.PojoDataType;
import com.dremio.exec.store.sys.OptionIterator.OptionValueWrapper;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationListManager.MaterializationInfo;
import com.dremio.exec.store.sys.accel.AccelerationListManager.ReflectionInfo;
import com.dremio.exec.work.WorkStats.FragmentInfo;
import com.dremio.exec.work.WorkStats.SlicingThreadInfo;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.task.TaskPool;
import com.google.common.collect.ImmutableList;

/**
 * An enumeration of all tables in Dremio's system ("sys") schema.
 * <p>
 *   OPTION, NODES and VERSION are local tables available on every SabotNode.
 *   MEMORY and THREADS are distributed tables with one record on every
 *   SabotNode.
 * </p>
 */
public enum SystemTable implements DatasetHandle, DatasetMetadata, PartitionChunkListing {

  OPTION("options", false, OptionValueWrapper.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new OptionIterator(sContext, context, OptionIterator.Mode.SYS_SESS);
    }
  },

  BOOT("boot", false, OptionValueWrapper.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new OptionIterator(sContext, context, OptionIterator.Mode.BOOT);
    }
  },

  NODES("nodes", true, NodeIterator.NodeInstance.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new NodeIterator(sContext, context);
    }
  },

  // TODO - should be possibly make this a distributed table so that we can figure out if
  // users have inconsistent versions installed across their cluster?
  VERSION("version", false, VersionIterator.VersionInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new VersionIterator();
    }
  },

  MEMORY("memory", true, MemoryIterator.MemoryInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new MemoryIterator(sContext, context);
    }
  },

  THREADS("threads", true, ThreadsIterator.ThreadSummary.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new ThreadsIterator(sContext, context);
    }
  },

  FRAGMENTS("fragments", true, FragmentInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new FragmentIterator(sContext, context);
    }
  },

  @SuppressWarnings("unchecked")
  REFLECTIONS("reflections", false, ReflectionInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return (Iterator<Object>) (Object) sContext.getAccelerationListManager().getReflections().iterator();
    }
  },

  @SuppressWarnings("unchecked")
  REFRESH("refreshes", false, AccelerationListManager.RefreshInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return (Iterator<Object>) (Object) sContext.getAccelerationListManager().getRefreshInfos().iterator();
    }
  },

  @SuppressWarnings("unchecked")
  MATERIALIZATIONS("materializations", false, MaterializationInfo.class){
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return (Iterator<Object>) (Object) sContext.getAccelerationListManager().getMaterializations().iterator();
    }
  },

  @SuppressWarnings("unchecked")
  SLICING_THREADS("slicing_threads", true, SlicingThreadInfo.class) {
    @Override
    public Iterator<Object> getIterator(SabotContext sContext, OperatorContext context) {
      final CoordinationProtos.NodeEndpoint endpoint = sContext.getEndpoint();
      final Iterable<TaskPool.ThreadInfo> threadInfos = sContext.getWorkStatsProvider().get().getSlicingThreads();
      return (Iterator<Object>) (Object) StreamSupport.stream(threadInfos.spliterator(), false)
        .map((info) -> new SlicingThreadInfo(
          endpoint.getAddress(),
          endpoint.getFabricPort(),
          info
        )).iterator();
    }
  },

  @SuppressWarnings("unchecked")
  DEPENDENCIES("dependencies", false, AccelerationListManager.DependencyInfo.class){
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return (Iterator<Object>) (Object) sContext.getAccelerationListManager().getReflectionDependencies().iterator();
    }
  },

  @SuppressWarnings("unchecked")
  SERVICES("services", false, ServicesIterator.ServiceSetInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new ServicesIterator(sContext);
    }
  };

  private static final long RECORD_COUNT = 100L;
  private static final long SIZE_IN_BYTES = 1000L;

  private static final DatasetStats DATASET_STATS =
      DatasetStats.of(RECORD_COUNT, ScanCostFactor.OTHER.getFactor());
  private static final ImmutableList<PartitionChunk> PARTITION_CHUNKS =
      ImmutableList.of(PartitionChunk.of(DatasetSplit.of(SIZE_IN_BYTES, RECORD_COUNT)));

  private final EntityPath entityPath;
  private final String tableName;
  private final boolean distributed;
  private final Class<?> pojoClass;

  SystemTable(final String tableName, final boolean distributed, final Class<?> pojoClass) {
    this.tableName = tableName;
    this.distributed = distributed;
    this.pojoClass = pojoClass;
    this.entityPath = new EntityPath(ImmutableList.of("sys", tableName));
  }

  public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
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

  @Override
  public EntityPath getDatasetPath() {
    return entityPath;
  }

  @Override
  public DatasetStats getDatasetStats() {
    return DATASET_STATS;
  }

  @Override
  public BatchSchema getRecordSchema() {
    RecordDataType dataType = new PojoDataType(pojoClass);
    RelDataType type = dataType.getRowType(JavaTypeFactoryImpl.INSTANCE);
    return BatchSchema.fromCalciteRowType(type);
  }

  @Override
  public Iterator<? extends PartitionChunk> iterator() {
    return PARTITION_CHUNKS.iterator();
  }
}
