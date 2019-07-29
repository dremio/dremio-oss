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
package com.dremio.plugins.elastic.planning;

import java.util.List;
import java.util.stream.Collectors;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

/**
 * Elasticsearch group scan.
 */
public class ElasticsearchGroupScan extends AbstractGroupScan {

  private final ElasticsearchScanSpec spec;
  private final long rowCountEstimate;

  public ElasticsearchGroupScan(
      OpProps props,
      ElasticsearchScanSpec spec,
      TableMetadata table,
      List<SchemaPath> columns,
      long rowCountEstimate
      ) {
    super(props, table, columns);
    this.spec = spec;
    this.rowCountEstimate = rowCountEstimate;
  }

  @JsonProperty("spec")
  public ElasticsearchScanSpec getScanSpec() {
    return spec;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    List<SplitAndPartitionInfo> splitWork = work
      .stream()
      .map(input -> input.getSplitAndPartitionInfo(true))
      .collect(Collectors.toList());

    return new ElasticsearchSubScan(
        getProps(),
        getDataset().getStoragePluginId(),
        spec,
        splitWork,
        getColumns(),
        Iterables.getOnlyElement(getReferencedTables()),
        getDataset().getSchema(),
        getDataset().getReadDefinition().getExtendedProperty()
        );
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.ELASTICSEARCH_SUB_SCAN_VALUE;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof ElasticsearchGroupScan)) {
      return false;
    }
    ElasticsearchGroupScan castOther = (ElasticsearchGroupScan) other;
    return Objects.equal(spec, castOther.spec) && Objects.equal(rowCountEstimate, castOther.rowCountEstimate);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(spec, rowCountEstimate);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("spec", spec).add("rowCountEstimate", rowCountEstimate).toString();
  }


}
