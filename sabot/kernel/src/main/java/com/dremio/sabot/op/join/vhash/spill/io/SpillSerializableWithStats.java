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

import com.dremio.exec.record.BatchSchema;
import com.dremio.sabot.op.join.vhash.spill.SpillStats;
import com.dremio.sabot.op.join.vhash.spill.pool.PageSupplier;
import com.dremio.sabot.op.sort.external.SpillManager;
import java.io.IOException;

/** Wrapper over SpillSerializable that can track stats. */
public class SpillSerializableWithStats implements SpillSerializable {
  private final SpillSerializable inner;
  private final SpillStats stats;
  private final boolean isBuild;

  public SpillSerializableWithStats(SpillSerializable inner, SpillStats stats, boolean isBuild) {
    this.inner = inner;
    this.stats = stats;
    this.isBuild = isBuild;
  }

  @Override
  public long writeChunkToStream(SpillChunk chunk, SpillManager.SpillOutputStream output)
      throws IOException {
    long startNanos = System.nanoTime();
    long ret = inner.writeChunkToStream(chunk, output);
    long bytesWritten = chunk.getUnpivotedSizeRounded() + chunk.getPivotedSizeRounded();
    long recordsWritten = chunk.getNumRecords();
    if (isBuild) {
      stats.addWriteBuildBytes(bytesWritten);
      stats.addWriteBuildRecords(recordsWritten);
      stats.addWriteBuildBatches(1);
    } else {
      stats.addWriteProbeBytes(bytesWritten);
      stats.addWriteProbeRecords(recordsWritten);
      stats.addWriteProbeBatches(1);
    }
    stats.addWriteNanos(System.nanoTime() - startNanos);
    return ret;
  }

  @Override
  public SpillChunk readChunkFromStream(
      PageSupplier pageSupplier,
      BatchSchema unpivotedColumnsSchema,
      SpillManager.SpillInputStream input)
      throws IOException {
    long startNanos = System.nanoTime();
    SpillChunk chunk = inner.readChunkFromStream(pageSupplier, unpivotedColumnsSchema, input);
    if (chunk != null) {
      long bytesRead = chunk.getUnpivotedSizeRounded() + chunk.getPivotedSizeRounded();
      long recordsRead = chunk.getNumRecords();
      if (isBuild) {
        stats.addReadBuildBytes(bytesRead);
        stats.addReadBuildRecords(recordsRead);
        stats.addReadBuildBatches(1);
      } else {
        stats.addReadProbeBytes(bytesRead);
        stats.addReadProbeRecords(recordsRead);
        stats.addReadProbeBatches(1);
      }
    }
    stats.addReadNanos(System.nanoTime() - startNanos);
    return chunk;
  }
}
