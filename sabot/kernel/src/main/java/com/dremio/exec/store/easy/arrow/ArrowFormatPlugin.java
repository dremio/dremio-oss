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

import java.io.IOException;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf.EasyDatasetSplitXAttr;

/**
 * {@link FormatPlugin} implementation for reading and writing Arrow format files in queries. Arrow buffers are
 * dumped to/read from file as it is from/to in-memory format. There is no endian-ness conversion,
 * so if the sharing files across different endian-ness machines is not supported.
 */
public class ArrowFormatPlugin extends EasyFormatPlugin<ArrowFormatPluginConfig> {
  public static final String ARROW_DEFAULT_NAME = "arrow";

  /**
   * Bytes of magic word for Arrow format files.
   */
  public static final String MAGIC_STRING = "DREMARROW1";
  public static final int MAGIC_STRING_LENGTH = MAGIC_STRING.getBytes().length;

  public static final int FOOTER_OFFSET_SIZE = Long.SIZE/Byte.SIZE; // Size of long in bytes

  /**
   * Standard signature constructor which is used when {@link FormatPlugin}s are constructored through reflection.
   * @param name
   * @param context
   * @param storageConfig
   * @param formatConfig
   * @param fsPlugin
   */
  public ArrowFormatPlugin(final String name, final SabotContext context, final ArrowFormatPluginConfig formatConfig, final FileSystemPlugin fsPlugin) {
    super(name, context, formatConfig, true, false, /* splittable = */ false, /* compressible = */ false,
        formatConfig.getDefaultExtensions(), ARROW_DEFAULT_NAME, fsPlugin);
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

  @Override
  public RecordReader getRecordReader(final OperatorContext context, final FileSystem dfs, EasyDatasetSplitXAttr splitAttributes, final List<SchemaPath> columns) throws ExecutionSetupException {
    final Path path = dfs.makeQualified(Path.of(splitAttributes.getPath()));
    return new ArrowRecordReader(context, dfs, path, columns);
  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.ARROW_SUB_SCAN_VALUE;
  }

  @Override
  public RecordWriter getRecordWriter(final OperatorContext context, final EasyWriter writer) throws IOException {
    return new ArrowRecordWriter(context, writer, (ArrowFormatPluginConfig) writer.getFormatConfig());
  }

  @Override
  public int getWriterOperatorType() {
    return CoreOperatorType.ARROW_WRITER_VALUE;
  }

  @Override
  public int getMaxFilesLimit() {
    return Math.toIntExact(getContext().getOptionManager().getOption(FileDatasetHandle.DFS_MAX_ARROW_FILES));
  }
}
