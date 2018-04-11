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
package com.dremio.dac.model.job;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.model.common.DACRuntimeException;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.datastore.indexed.IndexKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

/**
 * Json representation of job filters
 */
public class JobFilters {

  private static final Pattern ESCAPE_QUERY_PATTERN = Pattern.compile("[#&%]");
  private static final String CONTAINS = "contains";
  private String sort;
  private SortOrder order = SortOrder.DESCENDING;
  private Map<String, List<Object>> filters = new LinkedHashMap<>();

  public JobFilters addFilter(IndexKey indexKey, Object ...values) {
    final String key = indexKey.getShortName();
    if (filters.containsKey(key)) {
      filters.get(key).addAll(Arrays.asList(values));
    } else {
      filters.put(key, Lists.newArrayList(values));
    }
    return this;
  }

  // contains currently only supports single value
  public JobFilters addContainsFilter(String value) {
    filters.put(CONTAINS, Lists.<Object>newArrayList(value));
    return this;
  }

  public JobFilters setSort(String sort, SortOrder order) {
    this.sort = sort;
    this.order = order;
    return this;
  }

  public String toUrl() {
    try {
      String filterStr = new ObjectMapper().writeValueAsString(filters);
      String escapedFilter = PathUtils.encodeURIComponent(filterStr);
      if (sort != null) {
        return format("/jobs?filters=%s&sort=%s&order=%s", escapedFilter, PathUtils.encodeURIComponent(sort), order);
      } else {
        return format("/jobs?filters=%s", escapedFilter);
      }
    } catch (JsonProcessingException jpe) {
      throw new DACRuntimeException(jpe);
    }
  }
}
