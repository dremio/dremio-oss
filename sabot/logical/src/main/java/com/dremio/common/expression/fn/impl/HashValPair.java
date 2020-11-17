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

package com.dremio.common.expression.fn.impl;

/**
 * Pair of two hashes
 */
public class HashValPair {
  private long hash1;
  private long hash2;

  public HashValPair(long hash1, long hash2) {
    this.hash1 = hash1;
    this.hash2 = hash2;
  }

  public long getHash1() {
    return hash1;
  }

  public long getHash2() {
    return hash2;
  }

  @Override
  public String toString() {
    return "HashValPair{" +
      "hash1=" + hash1 +
      ", hash2=" + hash2 +
      '}';
  }
}
