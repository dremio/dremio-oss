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

import java.util.List;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilter;
import com.google.common.base.Preconditions;

/**
 * A POJO helper class for the protbuf struct CompositeColumnFilter.
 * It holds the deserialzied bloom filter which is not present in the generated pojo
 */
public class CompositeColumnFilter implements AutoCloseable {

  public enum RuntimeFilterType {
    BLOOM_FILTER,
    VALUE_LIST,
    VALUE_LIST_WITH_BLOOM_FILTER
  }

  private RuntimeFilterType filterType;
  private List<String> columnsList;
  private BloomFilter bloomFilter;
  private ValueListFilter valueList;

  private CompositeColumnFilter() {}

  public RuntimeFilterType getFilterType() {
    return filterType;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  public ValueListFilter getValueList() {
    return valueList;
  }

  public List<String> getColumnsList() {
    return columnsList;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(bloomFilter, valueList);
  }

  @Override
  public String toString() {
    return "CompositeColumnFilter{" +
            "filterType=" + filterType +
            ", columnsList=" + columnsList +
            '}';
  }



  public static class Builder {

    CompositeColumnFilter compositeColumnFilter = new CompositeColumnFilter();

    public Builder setFilterType(RuntimeFilterType filterType) {
      compositeColumnFilter.filterType = filterType;
      return this;
    }

    public Builder setBloomFilter(BloomFilter bloomFilter) {
      compositeColumnFilter.bloomFilter = bloomFilter;
      return this;
    }

    public Builder setValueList(ValueListFilter valueList) {
      compositeColumnFilter.valueList = valueList;
      return this;
    }

    public Builder setColumnsList(List<String> columnsList) {
      compositeColumnFilter.columnsList = columnsList;
      return this;
    }

    public Builder setProtoFields(ExecProtos.CompositeColumnFilter proto) {
      compositeColumnFilter.filterType = RuntimeFilterType.valueOf(proto.getFilterType().name());
      compositeColumnFilter.columnsList = proto.getColumnsList();
      return this;
    }

    public CompositeColumnFilter build() {
      Preconditions.checkArgument(compositeColumnFilter.columnsList != null && !compositeColumnFilter.columnsList.isEmpty(), "The columnsList is empty");
      Preconditions.checkArgument(compositeColumnFilter.filterType != null, "The filterType is empty");
      Preconditions.checkArgument((compositeColumnFilter.filterType == RuntimeFilterType.BLOOM_FILTER && compositeColumnFilter.bloomFilter != null) ||
        (compositeColumnFilter.filterType == RuntimeFilterType.VALUE_LIST && compositeColumnFilter.valueList != null), "The filter is empty");
      return compositeColumnFilter;
    }
  }
}
