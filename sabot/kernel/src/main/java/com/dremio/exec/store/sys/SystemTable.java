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
package com.dremio.exec.store.sys;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordDataType;
import com.dremio.exec.store.pojo.PojoDataType;
import com.dremio.exec.store.sys.OptionIterator.OptionValueWrapper;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationListManager.MaterializationInfo;
import com.dremio.exec.store.sys.accel.AccelerationListManager.ReflectionInfo;
import com.dremio.exec.work.WorkStats.FragmentInfo;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;

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

  QUERIES("queries", true, QueryIterator.QueryInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new QueryIterator(sContext, context);
    }
  },

  FRAGMENTS("fragments", true, FragmentInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return new FragmentIterator(sContext, context);
    }
  },

  REFLECTIONS("reflections", false, ReflectionInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return (Iterator<Object>) (Object) sContext.getAccelerationListManager().getReflections().iterator();
    }
  },

  REFRESH("refreshes", false, AccelerationListManager.RefreshInfo.class) {
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return (Iterator<Object>) (Object) sContext.getAccelerationListManager().getRefreshInfos().iterator();
    }
  },

  MATERIALIZATIONS("materializations", false, MaterializationInfo.class){
    @Override
    public Iterator<Object> getIterator(final SabotContext sContext, final OperatorContext context) {
      return (Iterator<Object>) (Object) sContext.getAccelerationListManager().getMaterializations().iterator();
    }
  }
  ;

  private final NamespaceKey key;
  private final String tableName;
  private final boolean distributed;
  private final Class<?> pojoClass;

  SystemTable(final String tableName, final boolean distributed, final Class<?> pojoClass) {
    this.tableName = tableName;
    this.distributed = distributed;
    this.pojoClass = pojoClass;
    this.key = new NamespaceKey(ImmutableList.<String>of("sys", tableName));
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

  public BatchSchema getSchema() {
    RecordDataType dataType = new PojoDataType(pojoClass);
    RelDataType type = dataType.getRowType(new JavaTypeFactoryImpl());
    return BatchSchema.fromCalciteRowType(type);
  }

  public SourceTableDefinition asTableDefinition(final DatasetConfig oldDataset) {
    return new SourceTableDefinition() {

      @Override
      public NamespaceKey getName() {
        return key;
      }

      @Override
      public DatasetConfig getDataset() throws Exception {
        final DatasetConfig dataset;
        if(oldDataset == null) {
          dataset = new DatasetConfig()
           .setFullPathList(key.getPathComponents())
          .setId(new EntityId(UUID.randomUUID().toString()))
          .setType(DatasetType.PHYSICAL_DATASET);

        } else {
          dataset = oldDataset;
        }

        return dataset
            .setName(key.getName())
            .setReadDefinition(new ReadDefinition()
                .setScanStats(new ScanStats().setRecordCount(100l)
                    .setScanFactor(ScanCostFactor.OTHER.getFactor())))
            .setOwner(SystemUser.SYSTEM_USERNAME)
            .setPhysicalDataset(new PhysicalDataset())
            .setRecordSchema(getSchema().toByteString())
            .setSchemaVersion(DatasetHelper.CURRENT_VERSION);
      }

      @Override
      public List<DatasetSplit> getSplits() throws Exception {
        // The split calculation is dynamic based on nodes at the time of execution, create a single split for the purposes here
        DatasetSplit split = new DatasetSplit();
        split.setSize(1l);
        split.setSplitKey("1");
        return ImmutableList.of(split);
      }

      @Override
      public boolean isSaveable() {
        return true;
      }

      @Override
      public DatasetType getType() {
        return DatasetType.PHYSICAL_DATASET;
      }};

  }
}
