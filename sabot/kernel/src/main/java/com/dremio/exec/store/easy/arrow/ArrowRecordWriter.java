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
package com.dremio.exec.store.easy.arrow;

import static com.dremio.exec.store.easy.arrow.ArrowFormatPlugin.MAGIC_STRING;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.arrow.vector.ValueVector;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.cache.VectorAccessibleSerializable;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.WritableBatch;
import com.dremio.exec.store.EventBasedRecordWriter;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.easy.arrow.ArrowFileFormat.ArrowFileFooter;
import com.dremio.exec.store.easy.arrow.ArrowFileFormat.ArrowFileMetadata;
import com.dremio.exec.store.easy.arrow.ArrowFileFormat.ArrowRecordBatchSummary;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * {@link RecordWriter} implementation for Arrow format files.
 */
public class ArrowRecordWriter implements RecordWriter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EventBasedRecordWriter.class);

  private final EasyWriter writerConfig;
  private final OperatorContext context;
  private final List<Path> listOfFilesCreated;
  private final ArrowFileFooter.Builder footerBuilder;

  private Path location;
  private String prefix;
  private String extension;
  private FileSystem fs;

  private int nextFileIndex = 0;

  private Path currentFile;
  private DataOutputStream currentFileOutputStream;
  private OutputEntryListener outputEntryListener;
  private WriteStatsListener writeStatsListener;
  private VectorAccessible incoming;

  private long recordCount;
  private String relativePath;

  public ArrowRecordWriter(OperatorContext context, final EasyWriter writerConfig,
                           ArrowFormatPluginConfig formatConfig) {
    final FragmentHandle handle = context.getFragmentHandle();

    this.writerConfig = writerConfig;
    this.context = context;
    this.listOfFilesCreated = Lists.newArrayList();
    this.footerBuilder = ArrowFileFooter.newBuilder();
    this.location = Path.of(writerConfig.getLocation());
    this.prefix = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    this.extension = formatConfig.outputExtension;
  }

  @Override
  public void setup(VectorAccessible incoming, OutputEntryListener outputEntryListener, WriteStatsListener writeStatsListener) throws IOException {
    Preconditions.checkArgument(incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE, "SelectionVector remover is not supported.");

    this.incoming = incoming;
    this.outputEntryListener = outputEntryListener;
    this.writeStatsListener = writeStatsListener;
    this.fs = writerConfig.getFormatPlugin().getFsPlugin().createFS(writerConfig.getProps().getUserName(), context);
    this.currentFile = fs.canonicalizePath(location.resolve(String.format("%s_%d.%s", prefix, nextFileIndex, extension)));
    this.relativePath = currentFile.getName();
    this.currentFileOutputStream = new DataOutputStream(fs.create(currentFile));
    listOfFilesCreated.add(currentFile);

    // write magic word bytes
    currentFileOutputStream.write(MAGIC_STRING.getBytes());

    for(final VectorWrapper<? extends ValueVector> vw : incoming) {
      Preconditions.checkArgument(!vw.isHyper(), "Writing hyper vectors to arrow format is not supported.");
      footerBuilder.addField(TypeHelper.getMetadata(vw.getValueVector()));
    }

    nextFileIndex++;
    recordCount = 0;
  }

  @Override
  public void startPartition(WritePartition partition) {
    if(!partition.isSinglePartition()){
      throw UserException.dataWriteError().message("Arrow writer doesn't support data partitioning.").build(logger);
    }
  }

  @Override
  public int writeBatch(int offset, int length) throws IOException {
    if(offset != 0 || length != incoming.getRecordCount()){
      throw UserException.dataWriteError().message("You cannot partition data written in Arrow format.").build(logger);
    }
    final int recordCount = incoming.getRecordCount();
    final long startOffset = currentFileOutputStream.size();

    final WritableBatch writableBatch = WritableBatch.getBatchNoHVWrap(recordCount, incoming, false /* isSv2 */);
    final VectorAccessibleSerializable serializer = new VectorAccessibleSerializable(writableBatch, null/*allocator*/);

    serializer.writeToStream(currentFileOutputStream);
    final long endOffset = currentFileOutputStream.size();

    final ArrowRecordBatchSummary summary =
        ArrowRecordBatchSummary
            .newBuilder()
            .setOffset(startOffset)
            .setRecordCount(recordCount)
            .build();

    footerBuilder.addBatch(summary);

    this.recordCount += recordCount;
    writeStatsListener.bytesWritten(endOffset - startOffset);

    return recordCount;
  }

  @Override
  public void abort() throws IOException {
    closeCurrentFile();
    for(final Path file : listOfFilesCreated) {
      fs.delete(file, true);
    }
  }

  private void closeCurrentFile() throws IOException {
    if (currentFileOutputStream != null) {
      // Save the footer starting offset
      final long footerStartOffset = currentFileOutputStream.size();

      // write the footer
      ArrowFileFooter footer = footerBuilder.build();
      footer.writeDelimitedTo(currentFileOutputStream);

      // write the foot offset
      currentFileOutputStream.writeLong(footerStartOffset);

      // write magic word bytes
      currentFileOutputStream.write(MAGIC_STRING.getBytes());

      final long fileSize = currentFileOutputStream.size();

      currentFileOutputStream.close();
      currentFileOutputStream = null;

      ArrowFileMetadata.Builder builder =
          ArrowFileMetadata.newBuilder()
              .setFooter(footer)
              .setRecordCount(recordCount)
              .setPath(relativePath)
              .setArrowMetadataVersion(org.apache.arrow.vector.types.MetadataVersion.DEFAULT.toFlatbufID());

      if(context.getNodeEndpointProvider() != null) {
        builder.setScreenNodeEndpoint(context.getNodeEndpointProvider().get());
      }

      ArrowFileMetadata lastFileMetadata = builder.build();

      outputEntryListener.recordsWritten(recordCount, fileSize, currentFile.toString(), lastFileMetadata.toByteArray(), null, null);
    }
  }

  @Override
  public void close() throws Exception {
    closeCurrentFile();
  }
}
