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
package com.dremio.exec.store.hive.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.iceberg.FileFormat;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.FileFormatException;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.WriterImpl;

import com.dremio.common.util.Closeable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.hive.HivePf4jPlugin;
import com.dremio.exec.store.hive.HiveFooterReaderTableFunction;
import com.dremio.exec.store.metadatarefresh.footerread.Footer;
import com.dremio.exec.store.metadatarefresh.footerread.FooterReader;
import com.dremio.exec.store.metadatarefresh.footerread.OrcFooter;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;

/**
 * ORC footer reader to get number of rows in a file
 * This code assumes incoming files are valid ORC files. it does not check for validation.
 */
public class HiveOrcFooterReader implements FooterReader {

  private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;
  private final FileSystem fs;
  private final BatchSchema tableSchema;
  private final int estimatedRecordSize;
  private final double compressionFactor;
  private final boolean readFooter;

  public HiveOrcFooterReader(BatchSchema tableSchema, FileSystem fs, OperatorContext context) {
    Preconditions.checkArgument(tableSchema != null, "Unexpected state");
    this.estimatedRecordSize = tableSchema.estimateRecordSize((int) context.getOptions().getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE),
            (int) context.getOptions().getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE));
    this.tableSchema = tableSchema;
    this.fs = fs;
    this.compressionFactor = 30f;
    this.readFooter = HiveFooterReaderTableFunction.isAccuratePartitionStatsNeeded(context);
  }

  @Override
  public Footer getFooter(String path, long fileSize) throws IOException {
    if (fileSize <= OrcFile.MAGIC.length()) {
      throw new FileFormatException("Not a valid ORC file " + path + " File Size is less than ORC MAGIC String");
    }
    long rows = 0;
    if (readFooter) {
      rows = getRowsFromFileFooter(path, fileSize);
    } else {
      rows = getEstimatedRowCount(fileSize);
    }
    return new OrcFooter(tableSchema, rows);
  }

  private long getEstimatedRowCount(long fileSize) {
    if (estimatedRecordSize != 0 && fileSize != 0) {
      return  (long)Math.ceil(fileSize * (compressionFactor / estimatedRecordSize));
    }
    return 0;
  }

  private long getRowsFromFileFooter(String path, long fileSize) throws IOException {
    try (final Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      FSInputStream inputStream = fs.open(Path.of(path));
      int readSize = (int) Math.min(fileSize, DIRECTORY_SIZE_GUESS);
      ByteBuffer bb = ByteBuffer.allocate(readSize);
      inputStream.setPosition(fileSize - readSize);
      readFully(inputStream, bb.array(), readSize);
      int psLen = bb.get(readSize - 1) & 0xff;

      int psOffset = readSize - 1 - psLen;
      assert bb.position() == 0;
      OrcProto.PostScript postScript = extractPostScriptViaByteBuffer(bb, psLen, psOffset);
      int footerLen = (int) postScript.getFooterLength();

      int totalFooterAndPsLen = footerLen + psLen + 1;
      int extra = Math.max(0, totalFooterAndPsLen - readSize);
      // Needs more bytes to read
      if (extra > 0) {
        ByteBuffer extraBuf = ByteBuffer.allocate(extra + readSize);
        inputStream.setPosition(fileSize - readSize - extra);
        readFully(inputStream, extraBuf.array(), extra);
        extraBuf.position(extra);
        //append with already read bytes
        bb.position(0);
        extraBuf.put(bb);
        bb = extraBuf;
        bb.position(0);
        readSize += extra;
        psOffset = readSize - 1 - psLen;
      }

      bb.position(psOffset - footerLen);
      ByteBuffer footerBuffer = bb.slice();
      OrcProto.Footer footer;
      int bufferSize = (int) postScript.getCompressionBlockSize();
      CompressionKind compressionKind = CompressionKind.valueOf(postScript.getCompression().name());
      CompressionCodec codec = WriterImpl.createCodec(compressionKind);
      footer = extractFooter(footerBuffer, 0, footerLen, codec, bufferSize);
      return footer.getNumberOfRows();
    }
  }

  private static OrcProto.Footer extractFooter(ByteBuffer bb, int footerAbsPos,
                                               int footerSize, CompressionCodec codec, int bufferSize) throws IOException {
    return OrcProto.Footer.parseFrom(InStream.createCodedInputStream("footer",
      Collections.singletonList(new BufferChunk(bb, 0)), footerSize, codec, bufferSize));
  }

  private void readFully(FSInputStream inputStream, byte[] bytes, int len) throws IOException {
    int offset = 0;
    while (offset < len) {
      int bytesRead = inputStream.read(bytes, offset, len - offset);
      offset += bytesRead;
    }
  }

  private static OrcProto.PostScript extractPostScriptViaByteBuffer(ByteBuffer bb,
                                                                    int psLen, int psAbsOffset) throws IOException {
    Preconditions.checkArgument(bb.hasArray(), "buffer should backed by an accessible byte array");
    bb.position(psAbsOffset);
    byte[] bytes = new byte[psLen];
    bb.get(bytes);
    return OrcProto.PostScript.parseFrom(bytes);
  }
}
