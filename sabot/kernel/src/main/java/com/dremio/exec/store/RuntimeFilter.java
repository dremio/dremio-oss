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
package com.dremio.exec.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.util.BloomFilter;

/**
 * A POJO helper class for the protobuf struct RuntimeFilter
 * The CompositeColumnFilter fields hold the deserialized bloom filter.
 */
public class RuntimeFilter implements AutoCloseable {
  private CompositeColumnFilter partitionColumnFilter;
  private List<CompositeColumnFilter> nonPartitionColumnFilter;
  private String senderInfo;

  public RuntimeFilter(CompositeColumnFilter partitionColumnFilter, List<CompositeColumnFilter> nonPartitionColumnFilter, String senderInfo) {
    this.partitionColumnFilter = partitionColumnFilter;
    this.nonPartitionColumnFilter = nonPartitionColumnFilter;
    this.senderInfo = senderInfo;
  }

  public CompositeColumnFilter getPartitionColumnFilter() {
    return partitionColumnFilter;
  }

  public List<CompositeColumnFilter> getNonPartitionColumnFilter() {
    return nonPartitionColumnFilter;
  }

  public String getSenderInfo() {
    return senderInfo;
  }

  public static RuntimeFilter getInstance(final ExecProtos.RuntimeFilter protoFilter,
                                          final BloomFilter bloomFilter,
                                          final String senderInfo) {
    final CompositeColumnFilter partitionColumnFilter = Optional.ofNullable(protoFilter.getPartitionColumnFilter())
            .map(proto -> new CompositeColumnFilter.Builder()
                    .setProtoFields(protoFilter.getPartitionColumnFilter())
                    .setBloomFilter(bloomFilter)
                    .build())
            .orElse(null);
    final List<CompositeColumnFilter> nonPartitionColumnFilter = Optional.ofNullable(protoFilter.getNonPartitionColumnFilterList()).orElse(new ArrayList<>(0))
            .stream()
            .map(proto -> new CompositeColumnFilter.Builder().setProtoFields(proto).build())
            .collect(Collectors.toList());

    return new RuntimeFilter(partitionColumnFilter, nonPartitionColumnFilter, senderInfo);
  }

  /**
   * Used for identifying duplicate filters.
   *
   * @param that
   * @return
   */
  public boolean isOnSameColumns(final RuntimeFilter that) {
    if (this.getPartitionColumnFilter() == null && that.getPartitionColumnFilter() == null) {
      return true;
    }
    if ((this.getPartitionColumnFilter() == null) != (that.getPartitionColumnFilter() == null)) {
      return false;
    }
    return Objects.equals(this.getPartitionColumnFilter().getColumnsList(), that.getPartitionColumnFilter().getColumnsList());
    // TODO: Extend for non partition columns
  }


  @Override
  public String toString() {
    return "RuntimeFilter{" +
            "partitionColumnFilter=" + partitionColumnFilter +
            ", nonPartitionColumnFilter=" + nonPartitionColumnFilter +
            ", senderInfo='" + senderInfo + '\'' +
            '}';
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(partitionColumnFilter);
  }
}
