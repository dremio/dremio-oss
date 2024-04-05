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

/** A concept of a range of values from two sets of records. Used to iterate over */
public abstract class DualRange implements AutoCloseable {

  /**
   * Whether there is available data to consume after the current data.
   *
   * @return
   */
  public abstract boolean hasNext();

  public abstract DualRange nextOutput();

  /**
   * If the current active range is empty. Useful for determining if an empty return occurred.
   *
   * @return True if no data present.
   */
  public abstract boolean isEmpty();

  /**
   * Inform the range that the next probe batch is now available.
   *
   * @param records Number of records in the probe batch.
   * @return
   */
  public abstract DualRange startNextProbe(int records);

  public abstract boolean isIndexRange();

  public IndexRange asIndexRange() {
    throw new UnsupportedOperationException();
  }

  public VectorRange asVectorRange() {
    throw new UnsupportedOperationException();
  }
}
