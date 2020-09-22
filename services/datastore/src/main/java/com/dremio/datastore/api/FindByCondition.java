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

import java.util.List;

import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import com.dremio.datastore.SearchTypes;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Configuration for finding values by condition.
 */
@JsonDeserialize(builder = ImmutableFindByCondition.Builder.class)
@Immutable
public interface FindByCondition {
  int DEFAULT_PAGE_SIZE = 5000;
  int DEFAULT_OFFSET = 0;
  int DEFAULT_LIMIT = Integer.MAX_VALUE;

  /**
   * Retrieves search condition.
   * @return condition.
   */
  SearchTypes.SearchQuery getCondition();

  /**
   * Retrieves sort.
   * @return sort.
   */
  List<SearchTypes.SearchFieldSorting> getSort();

  /**
   * Retrieves page size.
   * @return page size.
   */
  @Value.Default
  default int getPageSize() {
    return DEFAULT_PAGE_SIZE;
  }

  /**
   * Retrieves offset.
   * @return offset.
   */
  @Value.Default
  default int getOffset() {
    return DEFAULT_OFFSET;
  };

  /**
   * Retrieves limit.
   * @return limit
   */
  @Value.Default
  default int getLimit(){
    return DEFAULT_LIMIT;
  }
}
