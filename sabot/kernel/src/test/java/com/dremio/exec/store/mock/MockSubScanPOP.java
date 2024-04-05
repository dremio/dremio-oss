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
package com.dremio.exec.store.mock;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@JsonTypeName("mock-sub-scan")
public class MockSubScanPOP extends AbstractBase implements SubScan {
  private final String url;
  protected final List<MockGroupScanPOP.MockScanEntry> readEntries;

  @JsonCreator
  public MockSubScanPOP(
      @JsonProperty("props") OpProps props,
      @JsonProperty("url") String url,
      @JsonProperty("entries") List<MockGroupScanPOP.MockScanEntry> readEntries) {
    super(props);
    this.readEntries = readEntries;
    this.url = url;
  }

  public String getUrl() {
    return url;
  }

  @JsonProperty("entries")
  public List<MockGroupScanPOP.MockScanEntry> getReadEntries() {
    return readEntries;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new MockSubScanPOP(this.props, url, readEntries);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.MOCK_SUB_SCAN_VALUE;
  }

  @Deprecated
  public List<String> getTableSchemaPath() {
    return null;
  }

  @Override
  public List<List<String>> getReferencedTables() {
    return ImmutableList.of();
  }

  @Override
  public boolean mayLearnSchema() {
    return false;
  }

  @Override
  @JsonIgnore
  public List<SchemaPath> getColumns() {
    return Lists.transform(
        MockGroupScanPOP.getColumns(readEntries),
        new Function<String, SchemaPath>() {
          @Override
          public SchemaPath apply(String input) {
            return SchemaPath.getSimplePath(input);
          }
        });
  }

  @Override
  @JsonIgnore
  public BatchSchema getFullSchema() {
    return MockGroupScanPOP.getSchema(readEntries);
  }
}
