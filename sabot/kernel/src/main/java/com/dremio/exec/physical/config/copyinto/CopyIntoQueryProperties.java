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
package com.dremio.exec.physical.config.copyinto;

import com.dremio.exec.physical.config.ExtendedProperty;
import java.util.HashSet;
import java.util.Set;

public class CopyIntoQueryProperties implements ExtendedProperty {

  private OnErrorOption onErrorOption;
  private String storageLocation;
  private Set<CopyIntoFileLoadInfo.CopyIntoFileState> recordStateEvents = new HashSet<>();

  public CopyIntoQueryProperties() {
    // Needed for serialization-deserialization
  }

  public CopyIntoQueryProperties(OnErrorOption onErrorOption, String storageLocation) {
    this.onErrorOption = onErrorOption;
    this.storageLocation = storageLocation;
    setStateRecordEvents();
  }

  private void setStateRecordEvents() {
    recordStateEvents.clear();
    switch (onErrorOption) {
      case CONTINUE:
        addEventHistoryRecordsForState(CopyIntoFileLoadInfo.CopyIntoFileState.PARTIALLY_LOADED);
        addEventHistoryRecordsForState(CopyIntoFileLoadInfo.CopyIntoFileState.SKIPPED);
        break;
      case SKIP_FILE:
        addEventHistoryRecordsForState(CopyIntoFileLoadInfo.CopyIntoFileState.SKIPPED);
        break;
    }
  }

  public void setOnErrorOption(OnErrorOption onErrorOption) {
    this.onErrorOption = onErrorOption;
  }

  public void setStorageLocation(String storageLocation) {
    this.storageLocation = storageLocation;
  }

  public OnErrorOption getOnErrorOption() {
    return onErrorOption;
  }

  public String getStorageLocation() {
    return storageLocation;
  }

  public void addEventHistoryRecordsForState(CopyIntoFileLoadInfo.CopyIntoFileState state) {
    recordStateEvents.add(state);
  }

  public boolean shouldRecord(CopyIntoFileLoadInfo.CopyIntoFileState state) {
    return recordStateEvents.contains(state);
  }

  public Set<CopyIntoFileLoadInfo.CopyIntoFileState> getRecordStateEvents() {
    return recordStateEvents;
  }

  public void setRecordStateEvents(Set<CopyIntoFileLoadInfo.CopyIntoFileState> recordStateEvents) {
    this.recordStateEvents = recordStateEvents;
  }

  @Override
  public String toString() {
    return "CopyIntoQueryProperties{"
        + "onErrorOption="
        + onErrorOption
        + ", storageLocation='"
        + storageLocation
        + '\''
        + ", recordStateEvents="
        + recordStateEvents
        + '}';
  }

  public enum OnErrorOption {
    ABORT,
    CONTINUE,
    SKIP_FILE
  }
}
