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
package com.dremio.dac.explore.model;

import java.util.List;

import com.dremio.dac.explore.model.extract.Selection;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Replace values preview request
 */
public class ReplaceValuesPreviewReq {

  private final Selection selection;
  private final List<String> replacedValues;
  private final boolean replaceNull;

  @JsonCreator
  public ReplaceValuesPreviewReq(
      @JsonProperty("selection") Selection selection,
      @JsonProperty("replacedValues") List<String> replacedValues,
      @JsonProperty("replaceNull") boolean replaceNull) {
    super();
    this.selection = selection;
    this.replacedValues = replacedValues;
    this.replaceNull = replaceNull;
  }
  public Selection getSelection() {
    return selection;
  }
  public List<String> getReplacedValues() {
    return replacedValues;
  }
  public boolean isReplaceNull() {
    return replaceNull;
  }
}
