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
package com.dremio.exec.store.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import com.dremio.common.JSONOptions;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.calcite.logical.OldScanCrel;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.base.OldAbstractGroupScan;
import com.dremio.exec.planner.common.OldScanRelBase;
import com.dremio.exec.planner.logical.RelOptTableWrapper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.AbstractStoragePlugin;
import com.dremio.exec.store.ConversionContext;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePluginOptimizerRule;
import com.dremio.exec.store.hbase.HBaseSubScan.HBaseSubScanSpec;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.TableInstance;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class HBaseStoragePlugin extends AbstractStoragePlugin<ConversionContext.NamespaceConversionContext> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseStoragePlugin.class);
  private static final HBaseConnectionManager hbaseConnectionManager = HBaseConnectionManager.INSTANCE;

  private final SabotContext context;
  private final HBaseStoragePluginConfig storeConfig;
  private final HBaseConnectionKey connectionKey;

  private final String name;

  public HBaseStoragePlugin(HBaseStoragePluginConfig storeConfig, SabotContext context, String name)
      throws IOException {
    this.context = context;
    this.storeConfig = storeConfig;
    this.name = name;
    this.connectionKey = new HBaseConnectionKey();
  }

  public SabotContext getContext() {
    return this.context;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean folderExists(SchemaConfig schemaConfig, List folderPath) throws IOException {
    // TODO: add validation of new namespaces.
    return false;
  }

  @Override
  public RelNode getRel(final RelOptCluster cluster, final RelOptTable relOptTable, final ConversionContext.NamespaceConversionContext relContext) {
    final DatasetConfig datasetConfig = relContext.getDatasetConfig();
    BatchSchema schema = BatchSchema.fromDataset(datasetConfig);
    return new OldScanCrel(cluster, new RelOptTableWrapper(datasetConfig.getFullPathList(), relOptTable),
            cluster.traitSetOf(Convention.NONE),
            BatchSchema.fromDataset(datasetConfig).toCalciteRecordType(cluster.getTypeFactory()),
            new HBaseGroupScan(
                ImpersonationUtil.getProcessUserName()/*impersonation not supported for HBASE*/,
                this,
                new HBaseScanSpec(datasetConfig.getName()),
                OldAbstractGroupScan.ALL_COLUMNS,
                schema,
                datasetConfig.getFullPathList()
                ),
            null,
            OldScanRelBase.DEFAULT_ROW_COUNT_DISCOUNT
    );
  }

  @Override
  public DatasetConfig getDataset(List<String> tableSchemaPath, TableInstance tableInstance, SchemaConfig schemaConfig) {
    if (tableSchemaPath.size() <= 1) {
      // HBase schema path is incomplete
      return null;
    }
    String tableName = tableSchemaPath.get(1);
    BatchSchema batchSchema = sample(tableName);
    DatasetConfig datasetConfig = new DatasetConfig()
            .setFullPathList(tableSchemaPath)
            .setType(DatasetType.PHYSICAL_DATASET)
            .setName(tableName)
            .setOwner(schemaConfig.getUserName())
            .setRecordSchema(batchSchema.toByteString())
            .setSchemaVersion(DatasetHelper.CURRENT_VERSION)
            .setPhysicalDataset(new PhysicalDataset())
            ;
    try {
      context.getNamespaceService(schemaConfig.getUserName()).tryCreatePhysicalDataset(new NamespaceKey(tableSchemaPath), datasetConfig);
    } catch (NamespaceException e) {
      logger.warn("Failed to create dataset", e);
    }
    return datasetConfig;
  }

  private Set<String> getTableNames() {
    try(Admin admin = getConnection().getAdmin()) {
      HTableDescriptor[] tables = admin.listTables();
      Set<String> tableNames = Sets.newHashSet();
      for (HTableDescriptor table : tables) {
        tableNames.add(table.getNameAsString());
      }
      return tableNames;
    } catch (Exception e) {
      logger.warn("Failure while loading table names for database '{}'.", name, e.getCause());
      return Collections.emptySet();
    }
  }

  private BatchSchema sample(String tableName) {
    try (SampleMutator mutator = new SampleMutator(context)) {
      HBaseRecordReader reader = new HBaseRecordReader(
        getConnection(),
          new HBaseSubScanSpec(tableName, null, null, null, null, null),
          OldAbstractGroupScan.ALL_COLUMNS,
          null,
          true);
      reader.setNumRowsPerBatch(100);
      reader.setup(mutator);
      final int readCount = reader.next();
      if (readCount == 0) {
        // If the table is empty, then we just read the hbase column descriptors.
        SchemaBuilder builder = BatchSchema.newBuilder();
        try(Admin admin = getConnection().getAdmin()) {
          for (HColumnDescriptor col : admin.getTableDescriptor(TableName.valueOf(tableName)).getFamilies()) {
            String name = new String(col.getName());
            builder.addField(new Field(name, true, new ArrowType.Struct(), null));
          }
          builder.addField(new Field(HBaseRecordReader.ROW_KEY, true, Binary.INSTANCE, null));
          return builder.build();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      mutator.getContainer().buildSchema(SelectionVectorMode.NONE);
      return mutator.getContainer().getSchema();
    } catch (ExecutionSetupException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<DatasetConfig> listDatasets() {
    List<DatasetConfig> datasetConfigs = new ArrayList<>();
    for (String tableName : getTableNames()) {
      datasetConfigs.add(new DatasetConfig()
              .setFullPathList(ImmutableList.of(name, tableName))
              .setName(tableName)
              .setType(DatasetType.PHYSICAL_DATASET)
              .setOwner(ImpersonationUtil.getProcessUserName())
              .setPhysicalDataset(new PhysicalDataset())
      );
    }
    return datasetConfigs;
  }

  @Override
  public HBaseGroupScan getPhysicalScan(String userName, JSONOptions selection, List<String> tableSchemaPath, List<SchemaPath> columns) throws IOException {
    throw new UnsupportedOperationException();
    //    HBaseScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<HBaseScanSpec>() {});
    //    return new HBaseGroupScan(userName, this, scanSpec, columns, schema, tableSchemaPath);
  }

  @Override
  public HBaseStoragePluginConfig getConfig() {
    return storeConfig;
  }

  @Override
  public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    return ImmutableSet.of(HBasePushFilterIntoScan.FILTER_ON_SCAN, HBasePushFilterIntoScan.FILTER_ON_PROJECT);
  }

  @Override
  public void close() throws Exception {
    hbaseConnectionManager.closeConnection(connectionKey);
  }

  public Connection getConnection() {
    return hbaseConnectionManager.getConnection(connectionKey);
  }

  /**
   * An internal class which serves the key in a map of {@link HBaseStoragePlugin} => {@link Connection}.
   */
  class HBaseConnectionKey {

    private final ReentrantLock lock = new ReentrantLock();

    private HBaseConnectionKey() {}

    public void lock() {
      lock.lock();
    }

    public void unlock() {
      lock.unlock();
    }

    public Configuration getHBaseConf() {
      return storeConfig.getHBaseConf();
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((storeConfig == null) ? 0 : storeConfig.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null) {
        return false;
      } else if (getClass() != obj.getClass()) {
        return false;
      }

      HBaseStoragePlugin other = ((HBaseConnectionKey) obj).getHBaseStoragePlugin();
      if (name == null) {
        if (other.name != null) {
          return false;
        }
      } else if (!name.equals(other.name)) {
        return false;
      }
      if (storeConfig == null) {
        if (other.storeConfig != null) {
          return false;
        }
      } else if (!storeConfig.equals(other.storeConfig)) {
        return false;
      }
      return true;
    }

    private HBaseStoragePlugin getHBaseStoragePlugin() {
      return HBaseStoragePlugin.this;
    }

  }
}
