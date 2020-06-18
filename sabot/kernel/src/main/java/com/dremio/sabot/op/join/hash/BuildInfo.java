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
package com.dremio.sabot.op.join.hash;

import org.apache.arrow.memory.ArrowBuf;

public class BuildInfo implements AutoCloseable {
  // List of links. Logically it helps maintain a linked list of records with the same key value
  // Each link is 6 bytes.
  // First 4 bytes are used to identify the batch and remaining 2 bytes for record within the batch.
  private ArrowBuf links;

  // number of records in this batch
  int recordCount;

  public BuildInfo(ArrowBuf links, int recordCount) {
    this.links = links;
    this.recordCount = recordCount;
  }

  public ArrowBuf getLinks() {
    return links;
  }

  public int getRecordCount(){
    return recordCount;
  }

  @Override
  public void close() throws Exception {
    links.close();
  }
}
