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
package com.dremio.datastore.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.indexed.FilterIndexMapping;
import com.dremio.datastore.indexed.SearchFilterToQueryConverter;
import com.google.common.base.Objects;

/**
 * A KVStore that also maintains a index of documents for arbitrary retrieval.
 */
@Deprecated
public interface LegacyIndexedStore<K, V> extends LegacyKVStore<K, V> {

  String ID_FIELD_NAME = "_id";

  /**
   * Creates a lazy iterable over items that match the provided condition, in
   * the order requested. Exposing the appropriate keys and values. Note that
   * each iterator is independent and goes back to the source data to collect
   * data. As such, if you need to use multiple iterators, it is better to cache
   * the results. Note that this may also be internally paginating so different
   * calls to hasNext/next may have different performance characteristics.
   *
   * Note that two unexpected outcomes can occur with this iterator.
   *
   * (1) It is possible some of the values of this iterator will be null. This
   * can happen if the value is deleted around the time the iterator is created
   * and when the value is retrieved.
   *
   * (2) This iterator could return values that don't match the provided
   * conditions. This should be rare but can occur if the value was changed
   * around the time the iterator is created.
   *
   * @param find The condition to match.
   * @return A lazy iterable over the matching items.
   */
  Iterable<Entry<K, V>> find(LegacyFindByCondition find);

  /**
   * Provide a count of the number of documents that match each of the requested
   * conditions.
   *
   * @param conditions
   * @return
   */
  List<Integer> getCounts(SearchQuery... conditions);

  /**
   * Definition of how to find data by condition.
   */
  @Deprecated
  public static class LegacyFindByCondition {
    private SearchQuery condition;
    private int pageSize = 5000;
    private int limit = Integer.MAX_VALUE;
    private int offset = 0;
    private final List<SearchFieldSorting> sort = new ArrayList<>();

    public LegacyFindByCondition() {
    }

    public LegacyFindByCondition setCondition(String condition, FilterIndexMapping mapping) {
      this.condition = SearchFilterToQueryConverter.toQuery(condition, mapping);
      return this;
    }

    public LegacyFindByCondition setCondition(SearchQuery query) {
      this.condition = query;
      return this;
    }

    public LegacyFindByCondition setPageSize(int pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    public LegacyFindByCondition addSorting(SearchFieldSorting sorting) {
      this.sort.add(sorting);
      return this;
    }

    public LegacyFindByCondition addSortings(Collection<SearchFieldSorting> sort) {
      this.sort.addAll(sort);
      return this;
    }


    public LegacyFindByCondition setLimit(int limit){
      this.limit = limit;
      return this;
    }

    public LegacyFindByCondition setOffset(int offset){
      this.offset = offset;
      return this;
    }

    public SearchQuery getCondition() {
      return condition;
    }

    public int getPageSize() {
      return pageSize;
    }

    public List<SearchFieldSorting> getSort() {
      return sort;
    }

    public int getOffset() {
      return offset;
    }

    public int getLimit(){
      if(limit < pageSize){
        pageSize = limit;
      }
      return limit;
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof LegacyIndexedStore.LegacyFindByCondition)) {
        return false;
      }
      LegacyFindByCondition castOther = (LegacyFindByCondition) other;
      return Objects.equal(condition, castOther.condition) && Objects.equal(pageSize, castOther.pageSize)
          && Objects.equal(limit, castOther.limit) && Objects.equal(offset, castOther.offset)
          && Objects.equal(sort, castOther.sort);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(condition, pageSize, limit, offset, sort);
    }


  }

}
