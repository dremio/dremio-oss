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
package com.dremio.exec.planner.cost;

import com.google.common.base.Preconditions;

/**
 * An enumeration that holds relative costs between {@link com.dremio.exec.physical.base.GroupScan
 * group scans}.
 */
public class ScanCostFactor {

  public static final ScanCostFactor PARQUET =
      ScanCostFactor.of(1); // make parquet the cheapest persisted
  public static final ScanCostFactor ARROW = ScanCostFactor.of(1);
  public static final ScanCostFactor HIVE = ScanCostFactor.of(5);
  public static final ScanCostFactor EASY = ScanCostFactor.of(5); // json and text
  public static final ScanCostFactor JDBC = ScanCostFactor.of(10);
  public static final ScanCostFactor MONGO = ScanCostFactor.of(20);
  public static final ScanCostFactor ELASTIC = ScanCostFactor.of(20);
  public static final ScanCostFactor OTHER = ScanCostFactor.of(20);

  private final double factor;

  protected ScanCostFactor(final double factor) {
    Preconditions.checkArgument(factor > 0, "factor must be positive");
    this.factor = factor;
  }

  public double getFactor() {
    return factor;
  }

  public static ScanCostFactor of(final double factor) {
    return new ScanCostFactor(factor);
  }
}
