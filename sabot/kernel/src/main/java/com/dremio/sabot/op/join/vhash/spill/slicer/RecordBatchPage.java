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
package com.dremio.sabot.op.join.vhash.spill.slicer;

import java.util.List;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;

import com.dremio.exec.record.RecordBatchData;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;

/**
 * A record batch where all buffers are within a single page.
 */
public class RecordBatchPage extends RecordBatchData {
  private final Page page;

  RecordBatchPage(int recordCount, List<FieldVector> vectors, Page page) {
    super(vectors, recordCount);
    this.page = page;
    page.retain();
  }

  public Page getPage() {
    return page;
  }

  @Override
  public void close() {
    try {
      AutoCloseables.close(
        super::close,
        page::release);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


}
