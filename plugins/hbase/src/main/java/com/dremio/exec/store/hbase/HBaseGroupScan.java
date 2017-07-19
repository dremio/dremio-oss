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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.base.OldAbstractGroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.ScanStats;
import com.dremio.exec.physical.base.ScanStats.GroupScanProperty;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.hbase.HBaseSubScan.HBaseSubScanSpec;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.util.Utilities;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;

@JsonTypeName("hbase-scan")
public class HBaseGroupScan extends OldAbstractGroupScan<RegionWork> implements HBaseConstants {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseGroupScan.class);

  private static final Comparator<List<HBaseSubScanSpec>> LIST_SIZE_COMPARATOR = new Comparator<List<HBaseSubScanSpec>>() {
    @Override
    public int compare(List<HBaseSubScanSpec> list1, List<HBaseSubScanSpec> list2) {
      return list1.size() - list2.size();
    }
  };

  private static final Comparator<List<HBaseSubScanSpec>> LIST_SIZE_COMPARATOR_REV = Collections.reverseOrder(LIST_SIZE_COMPARATOR);

  /****************************************
   * Members used in equals()/hashCode()!
   * If you add a new member that needs to be in equality, add them eqauls() and hashCode().
   ****************************************/
  private HBaseStoragePluginConfig storagePluginConfig;
  private List<SchemaPath> columns;
  private HBaseScanSpec hbaseScanSpec;
  private boolean filterPushedDown = false;
  private HTableDescriptor hTableDesc;

  /****************************************
   * Members NOT used in equals()/hashCode()!
   ****************************************/
  private HBaseStoragePlugin storagePlugin;
  private Stopwatch watch = Stopwatch.createUnstarted();
  private TableStatsCalculator statsCalculator;
  private Map<Integer, List<HBaseSubScanSpec>> endpointFragmentMapping;
  private NavigableMap<HRegionInfo, ServerName> regionsToScan;
  private long scanSizeInBytes = 0;

  @JsonCreator
  public HBaseGroupScan(@JsonProperty("userName") String userName,
                        @JsonProperty("hbaseScanSpec") HBaseScanSpec hbaseScanSpec,
                        @JsonProperty("storage") HBaseStoragePluginConfig storagePluginConfig,
                        @JsonProperty("columns") List<SchemaPath> columns,
                        @JacksonInject StoragePluginRegistry pluginRegistry,
                        @JsonProperty("schema") BatchSchema schema,
                        @JsonProperty("tableSchemaPath") List<String> tablePath) throws IOException, ExecutionSetupException {
    this (userName, (HBaseStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), hbaseScanSpec, columns, schema, tablePath);
  }

  public HBaseGroupScan(String userName, HBaseStoragePlugin storagePlugin, HBaseScanSpec scanSpec,
      List<SchemaPath> columns,
      BatchSchema schema,
      List<String> tablePath) {
    super(userName, schema, tablePath);
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.hbaseScanSpec = scanSpec;
    this.columns = columns == null ? ALL_COLUMNS : columns;
    init();
  }

  /**
   * Private constructor, used for cloning.
   * @param that The HBaseGroupScan to clone
   */
  private HBaseGroupScan(HBaseGroupScan that) {
    super(that);
    this.columns = that.columns == null ? ALL_COLUMNS : that.columns;
    this.hbaseScanSpec = that.hbaseScanSpec;
    this.endpointFragmentMapping = that.endpointFragmentMapping;
    this.regionsToScan = that.regionsToScan;
    this.storagePlugin = that.storagePlugin;
    this.storagePluginConfig = that.storagePluginConfig;
    this.hTableDesc = that.hTableDesc;
    this.filterPushedDown = that.filterPushedDown;
    this.statsCalculator = that.statsCalculator;
    this.scanSizeInBytes = that.scanSizeInBytes;
  }

  @Override
  public HBaseGroupScan clone(List<SchemaPath> columns) {
    HBaseGroupScan newScan = new HBaseGroupScan(this);
    newScan.columns = columns == null ? ALL_COLUMNS : columns;;
    newScan.verifyColumns();
    return newScan;
  }


  @Override
  public Iterator<RegionWork> getSplits(ExecutionNodeMap executionNodes) {
    List<RegionWork> work = new ArrayList<>();

    for (Entry<HRegionInfo, ServerName> entry : regionsToScan.entrySet()) {
      long bytes = statsCalculator.getRegionSizeInBytes(entry.getKey().getRegionName());
      String name = entry.getValue().getHostname();
      NodeEndpoint endpoint = executionNodes.getEndpoint(name);
      if(endpoint != null){
        work.add(new RegionWork(entry.getKey(), bytes, new EndpointAffinity(endpoint, bytes)));
      }
    }
    return work.iterator();
  }

  @Override
  public SubScan getSpecificScan(List<RegionWork> work) throws ExecutionSetupException {
    List<HBaseSubScanSpec> readers = FluentIterable.from(work).transform(new Function<RegionWork, HBaseSubScanSpec>(){
      @Override
      public HBaseSubScanSpec apply(RegionWork input) {
        return regionInfoToSubScanSpec(input.getRegionInfo());
      }}).toList();

    return new HBaseSubScan(getUserName(), storagePlugin, storagePluginConfig, readers, columns, getSchema(), getTableSchemaPath());
  }

  private void init() {
    logger.debug("Getting region locations");
    TableName tableName = TableName.valueOf(hbaseScanSpec.getTableName());
    Connection conn = storagePlugin.getConnection();

    try (Admin admin = conn.getAdmin();
         RegionLocator locator = conn.getRegionLocator(tableName)) {
      this.hTableDesc = admin.getTableDescriptor(tableName);
      List<HRegionLocation> regionLocations = locator.getAllRegionLocations();
      statsCalculator = new TableStatsCalculator(conn, hbaseScanSpec, storagePlugin.getContext().getConfig(), storagePluginConfig);

      boolean foundStartRegion = false;
      regionsToScan = new TreeMap<>();
      for (HRegionLocation regionLocation : regionLocations) {
        HRegionInfo regionInfo = regionLocation.getRegionInfo();
        if (!foundStartRegion && hbaseScanSpec.getStartRow() != null && hbaseScanSpec.getStartRow().length != 0 && !regionInfo.containsRow(hbaseScanSpec.getStartRow())) {
          continue;
        }
        foundStartRegion = true;
        regionsToScan.put(regionInfo, regionLocation.getServerName());
        scanSizeInBytes += statsCalculator.getRegionSizeInBytes(regionInfo.getRegionName());
        if (hbaseScanSpec.getStopRow() != null && hbaseScanSpec.getStopRow().length != 0 && regionInfo.containsRow(hbaseScanSpec.getStopRow())) {
          break;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error getting region info for table: " + hbaseScanSpec.getTableName(), e);
    }
    verifyColumns();
  }

  private void verifyColumns() {
    if (ColumnUtils.isStarQuery(columns)) {
      return;
    }
    for (SchemaPath column : columns) {
      if (!(column.equals(ROW_KEY_PATH) || hTableDesc.hasFamily(HBaseUtils.getBytes(column.getRootSegment().getPath())))) {
        throw new RuntimeException(String.format("The column family '%s' does not exist in HBase table: %s .",
            column.getRootSegment().getPath(), hTableDesc.getNameAsString()));
      }
    }
  }

  @Override
  @JsonIgnore
  public StoragePlugin getPlugin() {
    return storagePlugin;
  }

  private HBaseSubScanSpec regionInfoToSubScanSpec(HRegionInfo ri) {
    HBaseScanSpec spec = hbaseScanSpec;
    return new HBaseSubScanSpec()
        .setTableName(spec.getTableName())
        .setRegionServer(regionsToScan.get(ri).getHostname())
        .setStartRow((!isNullOrEmpty(spec.getStartRow()) && ri.containsRow(spec.getStartRow())) ? spec.getStartRow() : ri.getStartKey())
        .setStopRow((!isNullOrEmpty(spec.getStopRow()) && ri.containsRow(spec.getStopRow())) ? spec.getStopRow() : ri.getEndKey())
        .setSerializedFilter(spec.getSerializedFilter());
  }

  private boolean isNullOrEmpty(byte[] key) {
    return key == null || key.length == 0;
  }


  @Override
  public int getMaxParallelizationWidth() {
    return regionsToScan.size();
  }

  @Override
  public ScanStats getScanStats() {
    long rowCount = (long) ((scanSizeInBytes / statsCalculator.getAvgRowSizeInBytes()) * (hbaseScanSpec.getFilter() != null ? 0.5 : 1));
    // the following calculation is not precise since 'columns' could specify CFs while getColsPerRow() returns the number of qualifier.
    float diskCost = scanSizeInBytes * ((columns == null || columns.isEmpty()) ? 1 : columns.size()/statsCalculator.getColsPerRow());
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, rowCount, 1, diskCost);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new HBaseGroupScan(this);
  }

  @JsonIgnore
  public HBaseStoragePlugin getStoragePlugin() {
    return storagePlugin;
  }

  @JsonIgnore
  public Configuration getHBaseConf() {
    return getStorageConfig().getHBaseConf();
  }

  @JsonIgnore
  public String getTableName() {
    return getHBaseScanSpec().getTableName();
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "HBaseGroupScan [HBaseScanSpec="
        + hbaseScanSpec + ", columns="
        + columns + "]";
  }

  @JsonProperty("storage")
  public HBaseStoragePluginConfig getStorageConfig() {
    return this.storagePluginConfig;
  }

  @JsonProperty
  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty
  public HBaseScanSpec getHBaseScanSpec() {
    return hbaseScanSpec;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public ScanCostFactor getScanCostFactor() {
    return ScanCostFactor.OTHER;
  }

  @JsonIgnore
  public void setFilterPushedDown(boolean b) {
    this.filterPushedDown = true;
  }

  @JsonIgnore
  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

  /**
   * Empty constructor, do not use, only for testing.
   */
  @VisibleForTesting
  public HBaseGroupScan() {
    super((String)null, BatchSchema.newBuilder().addField(new Field("random", true, new ArrowType.Struct(), null)).build(), null);
  }

  /**
   * Do not use, only for testing.
   */
  @VisibleForTesting
  public void setHBaseScanSpec(HBaseScanSpec hbaseScanSpec) {
    this.hbaseScanSpec = hbaseScanSpec;
  }

  /**
   * Do not use, only for testing.
   */
  @JsonIgnore
  @VisibleForTesting
  public void setRegionsToScan(NavigableMap<HRegionInfo, ServerName> regionsToScan) {
    this.regionsToScan = regionsToScan;
  }

  /****************************************
   * Add members to this, do not generate
   ****************************************/
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HBaseGroupScan that = (HBaseGroupScan) o;
    return filterPushedDown == that.filterPushedDown &&
        Objects.equal(storagePluginConfig, that.storagePluginConfig) &&
        Utilities.listsUnorderedEquals(columns, that.columns) &&
        Objects.equal(hbaseScanSpec, that.hbaseScanSpec) &&
        Objects.equal(hTableDesc, that.hTableDesc);
  }

  /****************************************
   * Add members to this, do not generate
   ****************************************/
  @Override
  public int hashCode() {
    return Objects.hashCode(
        storagePluginConfig,
        columns == null ? 0 : columns.size(),
        hbaseScanSpec,
        filterPushedDown,
        hTableDesc);
  }

}
