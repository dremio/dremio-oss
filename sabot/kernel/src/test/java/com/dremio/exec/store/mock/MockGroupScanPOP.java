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
package com.dremio.exec.store.mock;

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.ScanStats;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.schedule.CompleteWork;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

@JsonTypeName("mock-scan")
public class MockGroupScanPOP extends AbstractBase implements GroupScan<MockGroupScanPOP.MockScanEntry> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockGroupScanPOP.class);

  private final String url;
  protected final List<MockScanEntry> readEntries;
  private final BatchSchema schema;
  private final List<String> columns;

  @JsonCreator
  public MockGroupScanPOP(@JsonProperty("url") String url, @JsonProperty("entries") List<MockScanEntry> readEntries) {
    super((String) null);
    this.readEntries = readEntries == null ? ImmutableList.<MockScanEntry>of() : readEntries;
    this.schema = getSchema(this.readEntries);
    this.url = url;
    this.columns = getColumns(this.readEntries);
  }

  static BatchSchema getSchema(List<MockScanEntry> entries) {
    SchemaBuilder builder = BatchSchema.newBuilder();
    for(MockColumn e : entries.get(0).types){
      builder.addField(MajorTypeHelper.getFieldForNameAndMajorType(e.name, e.getMajorType()));
    }
    return builder.setSelectionVectorMode(SelectionVectorMode.NONE).build();
  }

  static List<String> getColumns(List<MockScanEntry> entries) {
    List<String> columns = new ArrayList<>();
    for(MockColumn e : entries.get(0).types){
      columns.add(e.name);
    }
    return columns;
  }

  public ScanStats getScanStats() {
    return ScanStats.TRIVIAL_TABLE;
  }

  public String getUrl() {
    return url;
  }

  @JsonProperty("entries")
  public List<MockScanEntry> getReadEntries() {
    return readEntries;
  }

  /****************************************
   * Dummy
   ****************************************/
  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  /****************************************
   * Dummy
   ****************************************/
  @Override
  public int hashCode() {
    return 0;
  }

  public static class MockScanEntry implements CompleteWork {

    private final int records;
    private final MockColumn[] types;
    private final int recordSize;

    @Override
    public int compareTo(CompleteWork o) {
      return Long.compare(getTotalBytes(), getTotalBytes());
    }

    @Override
    @JsonIgnore
    public long getTotalBytes() {
      return recordSize * records;
    }

    @Override
    @JsonIgnore
    public List<EndpointAffinity> getAffinity() {
      return ImmutableList.of();
    }

    @JsonCreator
    public MockScanEntry(@JsonProperty("records") int records, @JsonProperty("types") MockColumn[] types) {
      this.records = records;
      this.types = types;
      int size = 0;
      for (MockColumn dt : types) {
        size += TypeHelper.getSize(getArrowMinorType(dt.getMinorType()));
      }
      this.recordSize = size;
    }

    public int getRecords() {
      return records;
    }

    public MockColumn[] getTypes() {
      return types;
    }

    @Override
    public String toString() {
      return "MockScanEntry [records=" + records + ", columns=" + Arrays.toString(types) + "]";
    }
  }

  @JsonInclude(Include.NON_NULL)
  public static class MockColumn{
    @JsonProperty("type") public MinorType minorType;
    public String name;
    public DataMode mode;
    public Integer width;
    public Integer precision;
    public Integer scale;


    @JsonCreator
    public MockColumn(@JsonProperty("name") String name, @JsonProperty("type") MinorType minorType, @JsonProperty("mode") DataMode mode, @JsonProperty("width") Integer width, @JsonProperty("precision") Integer precision, @JsonProperty("scale") Integer scale) {
      this.name = name;
      this.minorType = minorType;
      this.mode = mode;
      this.width = width;
      this.precision = precision;
      this.scale = scale;
    }

    @JsonProperty("type")
    public MinorType getMinorType() {
      return minorType;
    }
    public String getName() {
      return name;
    }
    public DataMode getMode() {
      return mode;
    }
    public Integer getWidth() {
      return width;
    }
    public Integer getPrecision() {
      return precision;
    }
    public Integer getScale() {
      return scale;
    }

    @JsonIgnore
    public MajorType getMajorType() {
      MajorType.Builder b = MajorType.newBuilder();
      b.setMode(mode);
      b.setMinorType(minorType);
      if (precision != null) {
        b.setPrecision(precision);
      }
      if (width != null) {
        b.setWidth(width);
      }
      if (scale != null) {
        b.setScale(scale);
      }
      return b.build();
    }

    @Override
    public String toString() {
      return "MockColumn [minorType=" + minorType + ", name=" + name + ", mode=" + mode + "]";
    }

  }

  @Override
  public Iterator<MockScanEntry> getSplits(ExecutionNodeMap executionNodes) {
    return this.readEntries.iterator();
  }

  @Override
  public SubScan getSpecificScan(List<MockScanEntry> work) throws ExecutionSetupException {
    return new MockSubScanPOP(url, work);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return readEntries.size();
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new MockGroupScanPOP(url, readEntries);

  }

  @Override
  public List<SchemaPath> getColumns() {
    return Collections.emptyList();
  }

  @Override
  public String toString() {
    return "MockGroupScanPOP [url=" + url
        + ", readEntries=" + readEntries + "]";
  }

  @Override
  public List<String> getTableSchemaPath() {
    return ImmutableList.of("mock");
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitGroupScan(this, value);
  }

  @Override
  public int getOperatorType() {
    return 9001;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return ImmutableList.<PhysicalOperator>of().iterator();
  }

  @Override
  public int getMinParallelizationWidth() {
    return 1;
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.SOFT;
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext context) {
    return schema;
  }

}
