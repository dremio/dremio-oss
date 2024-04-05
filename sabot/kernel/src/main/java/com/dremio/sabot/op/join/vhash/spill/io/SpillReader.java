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

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.CloseableIterator;
import com.dremio.exec.record.BatchSchema;
import com.dremio.sabot.op.join.vhash.spill.pool.PageSupplier;
import com.dremio.sabot.op.sort.external.SpillManager.SpillFile;
import com.dremio.sabot.op.sort.external.SpillManager.SpillInputStream;
import com.google.common.base.Preconditions;
import java.io.IOException;

/**
 * Reader for spilled file, returns an iterator of chunks. Each chunk has both pivoted and unpivoted
 * data.
 */
public class SpillReader implements CloseableIterator<SpillChunk> {
  private final SpillFile spillFile;
  private final PageSupplier pageSupplier;
  private final BatchSchema unpivotedColumnsSchema;
  private final SpillSerializable serializable;
  private SpillChunk prefetchedChunk = null;
  private SpillInputStream inputStream;

  public SpillReader(
      SpillFile spillFile,
      SpillSerializable serializable,
      PageSupplier pageSupplier,
      BatchSchema unpivotedColumnsSchema) {
    this.spillFile = spillFile;
    this.serializable = serializable;
    this.pageSupplier = pageSupplier;
    this.unpivotedColumnsSchema = unpivotedColumnsSchema;
  }

  @Override
  public boolean hasNext() {
    if (prefetchedChunk != null) {
      return true;
    }
    prefetch();
    return prefetchedChunk != null;
  }

  @Override
  public SpillChunk next() {
    SpillChunk current = prefetchedChunk;
    // caller should release the chunk
    prefetchedChunk = null;
    return current;
  }

  public SpillChunk peek() {
    Preconditions.checkState(prefetchedChunk != null);
    return prefetchedChunk;
  }

  private void prefetch() {
    try {
      if (inputStream == null) {
        inputStream = spillFile.open(true);
      }
      prefetchedChunk =
          serializable.readChunkFromStream(pageSupplier, unpivotedColumnsSchema, inputStream);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(prefetchedChunk, inputStream);
  }
}
