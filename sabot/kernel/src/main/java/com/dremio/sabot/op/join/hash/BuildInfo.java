/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.join.hash;

import java.util.BitSet;

import com.dremio.exec.record.selection.SelectionVector4;

public class BuildInfo implements AutoCloseable {
  // List of links. Logically it helps maintain a linked list of records with the same key value
  private SelectionVector4 links;

  // List of bitvectors. Keeps track of records on the build side that matched a record on the probe side
  private BitSet keyMatchBitVector;

  // number of records in this batch
  int recordCount;

  public BuildInfo(SelectionVector4 links, BitSet keyMatchBitVector, int recordCount) {
    this.links = links;
    this.keyMatchBitVector = keyMatchBitVector;
    this.recordCount = recordCount;
  }

  public SelectionVector4 getLinks() {
    return links;
  }

  public BitSet getKeyMatchBitVector() {
    return keyMatchBitVector;
  }

  public int getRecordCount(){
    return recordCount;
  }

  @Override
  public void close() throws Exception {
    links.close();
  }
}