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
import com.dremio.exec.planner.sql.CalciteArrowHelper;
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
import com.dremio.exec.store.sys.accesscontrol.AccessControlListingManager;
import com.dremio.exec.store.sys.accesscontrol.SysTableMembershipInfo;
import com.dremio.exec.store.sys.accesscontrol.SysTablePrivilegeInfo;
import com.dremio.exec.store.sys.accesscontrol.SysTableRoleInfo;
import com.dremio.exec.store.sys.statistics.StatisticsListManager;
import com.dremio.exec.work.CacheManagerDatasetInfo;
import com.dremio.exec.work.CacheManagerFilesInfo;
import com.dremio.exec.work.CacheManagerMountPointInfo;
import com.dremio.exec.work.CacheManagerStoragePluginInfo;
import com.dremio.exec.work.WorkStats.FragmentInfo;
import com.dremio.exec.work.WorkStats.SlicingThreadInfo;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.task.TaskPool;
import com.google.common.base.Throwables;
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

  OPTION(false, OptionValueWrapper.class, "options") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new OptionIterator(sContext, context, OptionIterator.Mode.SYS_SESS);
    }
  },

  BOOT(false, OptionValueWrapper.class, "boot") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new OptionIterator(sContext, context, OptionIterator.Mode.BOOT);
    }
  },

  NODES(true, NodeInstance.class, "nodes") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new NodeIterator(sContext, context);
    }
  },

  // TODO - should be possibly make this a distributed table so that we can figure out if
  // users have inconsistent versions installed across their cluster?
  VERSION(false, VersionIterator.VersionInfo.class, "version") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new VersionIterator();
    }
  },

  MEMORY(true, MemoryIterator.MemoryInfo.class, "memory") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new MemoryIterator(sContext, context);
    }
  },

  THREADS(true, ThreadsIterator.ThreadSummary.class, "threads") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new ThreadsIterator(sContext, context);
    }
  },

  FRAGMENTS(true, FragmentInfo.class, "fragments") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new FragmentIterator(sContext, context);
    }
  },

  REFLECTIONS(false, ReflectionInfo.class, "reflections") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return sContext.getAccelerationListManager().getReflections();
    }
  },

  REFRESH(false, AccelerationListManager.RefreshInfo.class, "refreshes") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return sContext.getAccelerationListManager().getRefreshInfos();
    }
  },

  MATERIALIZATIONS(false, MaterializationInfo.class, "materializations") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return sContext.getAccelerationListManager().getMaterializations();
    }
  },

  SLICING_THREADS(true, SlicingThreadInfo.class, "slicing_threads") {
    @Override
    public Iterator<?> getIterator(SabotContext sContext, OperatorContext context) {
      final CoordinationProtos.NodeEndpoint endpoint = sContext.getEndpoint();
      final Iterable<TaskPool.ThreadInfo> threadInfos = sContext.getWorkStatsProvider().get().getSlicingThreads();
      return StreamSupport.stream(threadInfos.spliterator(), false)
        .map((info) -> new SlicingThreadInfo(
          endpoint.getAddress(),
          endpoint.getFabricPort(),
          info
        )).iterator();
    }
  },

  DEPENDENCIES(false, AccelerationListManager.DependencyInfo.class, "dependencies") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return sContext.getAccelerationListManager().getReflectionDependencies();
    }
  },

  SERVICES(false, ServicesIterator.ServiceSetInfo.class, "services") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new ServicesIterator(sContext);
    }
  },

  CACHE_MANAGER_MOUNT_POINTS(true, CacheManagerMountPointInfo.class, "cache", "mount_points") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new CacheManagerMountPointIterator(sContext, context);
    }
  },

  CACHE_MANAGER_STORAGE_PLUGINS(true, CacheManagerStoragePluginInfo.class, "cache", "storage_plugins") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new CacheManagerStoragePluginIterator(sContext, context);
    }
  },

  CACHE_MANAGER_DATASETS(true, CacheManagerDatasetInfo.class, "cache", "datasets") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new CacheManagerDatasetIterator(sContext);
    }
  },

  CACHE_MANAGER_FILES(true, CacheManagerFilesInfo.class, "cache", "objects") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new CacheManagerFilesIterator(sContext);
    }
  },

  TIMEZONE_ABBREVIATIONS(false, TimezoneAbbreviations.TimezoneAbbr.class, "timezone_abbrevs") {
    @Override
    public Iterator<?> getIterator(final SabotContext sabotContext, final OperatorContext operatorContext) {
      return TimezoneAbbreviations.getIterator();
    }
  },

  TIMEZONE_NAMES(false, TimezoneNames.TimezoneRegion.class, "timezone_names") {
    @Override
    public Iterator<?> getIterator(final SabotContext sabotContext, final OperatorContext operatorContext) {
      return TimezoneNames.getIterator();
    }
  },

  ROLES(false, SysTableRoleInfo.class, "roles") {
    @Override
    public Iterator<?> getIterator(final SabotContext sabotContext, final OperatorContext operatorContext) {
      try {
        final AccessControlListingManager accessControlListingManager = sabotContext.getAccessControlListingManager();
        if (accessControlListingManager == null) {
          throw new IllegalAccessException("Unable to retrieve sys.roles.");
        }

        return accessControlListingManager.getRoleInfo().iterator();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  },

  PRIVILEGES(false, SysTablePrivilegeInfo.class, "privileges") {
    @Override
    public Iterator<?> getIterator (final SabotContext sabotContext, final OperatorContext operatorContext){
      try {
        final AccessControlListingManager accessControlListingManager = sabotContext.getAccessControlListingManager();
        if (accessControlListingManager == null) {
          throw new IllegalAccessException("Unable to retrieve sys.privileges.");
        }

        return accessControlListingManager.getPrivilegeInfo().iterator();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  },

  MEMBERSHIP(false, SysTableMembershipInfo.class, "membership") {
    @Override
    public Iterator<?> getIterator(final SabotContext sabotContext, final OperatorContext operatorContext) {
      try {
        final AccessControlListingManager accessControlListingManager = sabotContext.getAccessControlListingManager();
        if (accessControlListingManager == null) {
          throw new IllegalAccessException("Unable to retrieve sys.membership.");
        }

        return accessControlListingManager.getMembershipInfo().iterator();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  },

  TABLE_STATISTICS(false, StatisticsListManager.StatisticsInfo.class, "table_statistics") {
    @Override
    public Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context) {
      return sContext.getStatisticsListManagerProvider().get().getStatisticsInfos().iterator();
    }
  };

  private static final long RECORD_COUNT = 100L;
  private static final long SIZE_IN_BYTES = 1000L;

  private static final DatasetStats DATASET_STATS =
      DatasetStats.of(RECORD_COUNT, ScanCostFactor.OTHER.getFactor());
  private static final ImmutableList<PartitionChunk> PARTITION_CHUNKS =
      ImmutableList.of(PartitionChunk.of(DatasetSplit.of(SIZE_IN_BYTES, RECORD_COUNT)));

  private EntityPath entityPath;
  private final boolean distributed;
  private final Class<?> pojoClass;

  SystemTable(final boolean distributed, final Class<?> pojoClass, final String component, final String... components) {
    this.distributed = distributed;
    this.pojoClass = pojoClass;

    this.entityPath = new EntityPath(ImmutableList.<String>builder()
        .add("sys")
        .add(component)
        .add(components)
        .build());
  }

  public abstract Iterator<?> getIterator(final SabotContext sContext, final OperatorContext context);

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

  public void setDatasetPath(EntityPath entityPath) {
    this.entityPath = entityPath;
  }

  @Override
  public DatasetStats getDatasetStats() {
    return DATASET_STATS;
  }

  @Override
  public BatchSchema getRecordSchema() {
    RecordDataType dataType = new PojoDataType(pojoClass);
    RelDataType type = dataType.getRowType(JavaTypeFactoryImpl.INSTANCE);
    return CalciteArrowHelper.fromCalciteRowType(type);
  }

  @Override
  public Iterator<? extends PartitionChunk> iterator() {
    return PARTITION_CHUNKS.iterator();
  }
}
