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
package com.dremio.dac.explore.model;

import java.util.List;

import com.dremio.dac.util.JSONUtil;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The list of recent history items
 */
@JsonIgnoreProperties(value={"tipVersion"}, allowGetters=true)
public class History {

  // occ version
  private final Long version;
  // This list goes from oldest history item to the newest. Therefore
  // the tip version is available in the last item in the list.
  private final List<HistoryItem> items;
  // This can trail behind the tip of the history if a user selects a previous
  // point in the history. We still want to show the future history items
  // to allow them to navigate "Back to the Future" (TM).
  private final DatasetVersion currentDatasetVersion;

  private final boolean edited;

  @JsonCreator
  public History(
      @JsonProperty("items") List<HistoryItem> items,
      @JsonProperty("currentDatasetVersion") DatasetVersion currentDatasetVersion,
      @JsonProperty("version") Long version,
      @JsonProperty("isEdited") boolean edited
      ) {
    super();
    this.items = items;
    this.currentDatasetVersion = currentDatasetVersion;
    this.version = version;
    this.edited = edited;
  }

  @JsonGetter
  public DatasetVersion getTipVersion() {
    return items.get(items.size() - 1).getDatasetVersion();
  }

  public List<HistoryItem> getItems() {
    return items;
  }

  public DatasetVersion getCurrentDatasetVersion() {
    return currentDatasetVersion;
  }

  @JsonProperty("isEdited")
  public boolean getEdited() {
    return edited;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }
}
