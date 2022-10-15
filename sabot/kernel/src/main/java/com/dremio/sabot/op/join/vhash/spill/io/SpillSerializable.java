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

import java.io.IOException;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.FieldVector;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.util.RoundUtil;
import com.dremio.sabot.op.join.vhash.spill.pool.PageSupplier;
import com.dremio.sabot.op.sort.external.SpillManager.SpillInputStream;
import com.dremio.sabot.op.sort.external.SpillManager.SpillOutputStream;

public interface SpillSerializable {
  /**
   * Serialize and write a chunk to the output stream.
   *
   * @param chunk spill chunk
   * @param output output stream
   * @return number of bytes written
   * @throws IOException
   */
  long writeChunkToStream(SpillChunk chunk, SpillOutputStream output) throws IOException;

  /**
   * Read a deserialize a chunk from the input stream.
   *
   * @param pageSupplier supplier for pages
   * @param unpivotedColumnsSchema schema for columns that need to be unpivoted
   * @param input input stream
   * @return spill chunk
   * @throws IOException
   */
  SpillChunk readChunkFromStream(PageSupplier pageSupplier, BatchSchema unpivotedColumnsSchema, SpillInputStream input) throws IOException;

  static int computeUnpivotedSizeRounded(VectorAccessible va) {
    int total = 0;
    for (VectorWrapper<?> wrapper : va) {
      for (ArrowBuf buf : ((FieldVector) wrapper.getValueVector()).getFieldBuffers()) {
        // we do a roundup for 64-bit alignment since that is a more accurate value for the size of buffers that
        // need to be allocated for unload/merge.
        total += RoundUtil.round8up(LargeMemoryUtil.checkedCastToInt(buf.readableBytes()));
      }
    }
    return total;
  }
}
