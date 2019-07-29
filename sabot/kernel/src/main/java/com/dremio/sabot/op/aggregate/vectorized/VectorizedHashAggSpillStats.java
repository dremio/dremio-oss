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
package com.dremio.sabot.op.aggregate.vectorized;

/**
 * A subset of {@link VectorizedHashAggOperator} stats used
 * for verification in unit tests.
 */
public class VectorizedHashAggSpillStats {
  private int spills;
  private int ooms;
  private int iterations;
  private int recursionDepth;

  public void setSpills(final int spills) {
    this.spills = spills;
  }

  public int getSpills() {
    return spills;
  }

  public void setOoms(final int ooms) {
    this.ooms = ooms;
  }

  public int getOoms() {
    return ooms;
  }

  public void setIterations(final int iterations) {
    this.iterations = iterations;
  }

  public int getIterations() {
    return iterations;
  }

  public void setRecursionDepth(final int depth) {
    this.recursionDepth = depth;
  }

  public int getRecursionDepth() {
    return recursionDepth;
  }
}
