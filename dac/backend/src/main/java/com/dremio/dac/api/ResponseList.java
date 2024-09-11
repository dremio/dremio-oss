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
package com.dremio.dac.api;

import com.dremio.dac.server.GenericErrorMessage;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Generic list model for the public REST API. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ResponseList<T> {
  private final List<T> data = new ArrayList<>();
  private final List<Object> included = new ArrayList<>();
  private final List<GenericErrorMessage> errors = new ArrayList<>();

  public ResponseList() {}

  public ResponseList(Collection<T> data) {
    this.data.addAll(data);
  }

  public void add(T item) {
    data.add(item);
  }

  public void addIncluded(Object item) {
    included.add(item);
  }

  public void addError(GenericErrorMessage error) {
    errors.add(error);
  }

  public List<T> getData() {
    return data;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<Object> getIncluded() {
    return included;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<GenericErrorMessage> getErrors() {
    return errors;
  }
}
