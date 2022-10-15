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
package org.apache.arrow.vector;

/**
 * Class holds information about a single record (list) in a list vector
 */
public class ListVectorRecordInfo {

  public ListVectorRecordInfo(final int size, final int numOfElements) {
    this.size = size;
    this.numOfElements = numOfElements;
  }

  private int size;
  private int numOfElements;

  public int getSize() {
    return size;
  }

  public int getNumOfElements() {
    return numOfElements;
  }
}
