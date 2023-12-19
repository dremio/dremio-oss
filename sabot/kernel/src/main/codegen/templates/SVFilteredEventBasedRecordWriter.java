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
<@pp.dropOutputFile />
<@pp.changeOutputFile name="/com/dremio/exec/store/SVFilteredEventBasedRecordWriter.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.store;

import java.io.IOException;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.store.EventBasedRecordWriter;
import com.dremio.exec.store.RowBasedRecordWriter;
import com.google.common.base.Preconditions;

public class SVFilteredEventBasedRecordWriter extends EventBasedRecordWriter {
  public SVFilteredEventBasedRecordWriter(VectorAccessible batch, RowBasedRecordWriter recordWriter) throws IOException {
    super(batch, recordWriter);
    Preconditions.checkNotNull(batch.getSelectionVector2());
  }

  public void setBatch(VectorAccessible batch) {
    this.batch = batch;
  }

  @Override
  public int write(int offset, int length) throws IOException {
    SelectionVector2 selectionVector2 = batch.getSelectionVector2();
    final int max = offset + length;
    int writeCount = 0;
    for (int i = 0; i < selectionVector2.getCount(); i++) {
      char index = selectionVector2.getIndex(i);
      if (index < max) {
        writeCount += writeOneRecord(index);
      }
    }
    return writeCount;
  }
}
