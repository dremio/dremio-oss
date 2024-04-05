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

package com.dremio.dac.model.scripts;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** PaginatedResponse class */
public class PaginatedResponse<T> {
  private final Long total;
  private final List<T> data;

  @JsonCreator
  public PaginatedResponse(@JsonProperty("total") long total, @JsonProperty("data") List<T> data) {
    this.total = total;
    this.data = data;
  }

  public Long getTotal() {
    return total;
  }

  public List<T> getData() {
    return data;
  }
}
