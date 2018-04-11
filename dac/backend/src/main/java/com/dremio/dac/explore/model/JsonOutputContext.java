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

/**
 * Context to keep track of space usage and value truncation events.
 */
public class JsonOutputContext {
  private int remaining;
  private boolean truncated;

  public JsonOutputContext(int remaining) {
    this.remaining = remaining;
  }

  public int getRemaining() {
    return remaining;
  }

  public void used(int used) {
    remaining -= used;
  }

  public void setTruncated() {
    truncated = true;
  }

  public boolean isTruncated() {
    return truncated;
  }

  public boolean okToWrite() {
    return remaining > 0;
  }

  public void reset(int maxCellValue) {
    remaining = maxCellValue;
    truncated = false;
  }
}
