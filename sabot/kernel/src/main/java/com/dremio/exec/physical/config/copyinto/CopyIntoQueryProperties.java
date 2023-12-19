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

public final class CopyIntoQueryProperties implements ExtendedProperty {

  private OnErrorOption onErrorOption;
  private String storageLocation;

  public CopyIntoQueryProperties() {
    // Needed for serialization-deserialization
  }

  public CopyIntoQueryProperties(OnErrorOption onErrorOption, String storageLocation) {
    this.onErrorOption = onErrorOption;
    this.storageLocation = storageLocation;
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

  @Override
  public String toString() {
    return "ReaderOptions{" +
      "onErrorOption='" + onErrorOption + "'\n" +
      "storageLocation='" + storageLocation + "'\n" +
      '}';
  }

  public enum OnErrorOption {
    ABORT, CONTINUE
  }
}
