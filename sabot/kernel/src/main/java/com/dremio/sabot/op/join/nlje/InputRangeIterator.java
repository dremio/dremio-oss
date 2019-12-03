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
package com.dremio.sabot.op.join.nlje;

public interface InputRangeIterator extends AutoCloseable {

  int next();
  boolean hasNext();

  @Override
  default void close() throws Exception {}

  /**
   * Reset the range given a new set of probe records.
   * @param probeRecords Number of probe records.
   */
  void startNextProbe(int probeRecords);

  InputRangeIterator EMPTY = new InputRangeIterator() {

    @Override
    public int next() {
      throw new IllegalStateException("nothing is available");
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public void startNextProbe(int probeRecords) {
    }};
}
