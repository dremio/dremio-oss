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
package com.dremio.sabot.op.join.vhash.spill.io;

import com.dremio.sabot.op.sort.external.SpillManager;

/** Descriptor for a spill file. */
public class SpillFileDescriptor {
  private SpillManager.SpillFile file;
  private long numRecords;
  private long sizeInBytes;

  public SpillFileDescriptor(SpillManager.SpillFile file) {
    this.file = file;
  }

  public void update(long numRecords, long sizeInBytes) {
    this.numRecords = numRecords;
    this.sizeInBytes = sizeInBytes;
  }

  public SpillManager.SpillFile getFile() {
    return file;
  }

  public long getNumRecords() {
    return numRecords;
  }

  public long getSizeInBytes() {
    return sizeInBytes;
  }
}
