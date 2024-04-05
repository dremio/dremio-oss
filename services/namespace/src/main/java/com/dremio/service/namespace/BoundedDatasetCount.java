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
package com.dremio.service.namespace;

/** BoundedDatasetCount represents a dataset count that was time/count bound. */
public class BoundedDatasetCount {
  private final int count;
  private final boolean timeBound;
  private final boolean countBound;

  public static final long SEARCH_TIME_LIMIT_MS = 1000L;
  public static final int COUNT_LIMIT_TO_STOP_SEARCH = 500;

  public BoundedDatasetCount(int count, boolean timeBound, boolean countBound) {
    this.count = count;
    this.timeBound = timeBound;
    this.countBound = countBound;
  }

  public int getCount() {
    return count;
  }

  public boolean isTimeBound() {
    return timeBound;
  }

  public boolean isCountBound() {
    return countBound;
  }
}
