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
package com.dremio.exec.planner.acceleration;

import java.util.Objects;

public class IncrementalUpdateSettings {
  public static final IncrementalUpdateSettings NON_INCREMENTAL = new IncrementalUpdateSettings(false, null);
  public static final IncrementalUpdateSettings FILE_BASED = new IncrementalUpdateSettings(true, null);

  private boolean incremental;
  private String updateField;

  public IncrementalUpdateSettings(boolean incremental, String updateField) {
    this.incremental = incremental;
    this.updateField = updateField;
  }

  public IncrementalUpdateSettings columnBased(String columnName) {
    return new IncrementalUpdateSettings(true, updateField);
  }

  public boolean isIncremental() {
    return incremental;
  }

  public String getUpdateField() {
    return updateField;
  }

  public boolean isFileBasedUpdate() {
    return incremental && updateField == null;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || o.getClass() != this.getClass()) {
      return false;
    }
    final IncrementalUpdateSettings that = (IncrementalUpdateSettings) o;
    return (incremental == that.incremental) &&
        Objects.equals(updateField, that.updateField);
  }

  public int hashcode() {
    return Objects.hash(incremental,updateField);
  }
}
