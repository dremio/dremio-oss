/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.easy.sequencefile;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.file.proto.EasyDatasetSplitXAttr;

public class SequenceFileFormatPlugin extends EasyFormatPlugin<SequenceFileFormatConfig> {
  public SequenceFileFormatPlugin(String name, SabotContext context, StoragePluginConfig storageConfig, FileSystemPlugin fsPlugin) {
    this(name, context, storageConfig, new SequenceFileFormatConfig(), fsPlugin);
  }

  public SequenceFileFormatPlugin(String name, SabotContext context, StoragePluginConfig storageConfig, SequenceFileFormatConfig formatConfig, FileSystemPlugin fsPlugin) {
    super(name, context, storageConfig, formatConfig,
      true, false, /* splittable = */ true, /* compressible = */ true,
      formatConfig.getExtensions(), "sequencefile", fsPlugin);
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context,
                                      FileSystemWrapper dfs,
                                      EasyDatasetSplitXAttr splitAttributes,
                                      List<SchemaPath> columns) throws ExecutionSetupException {
    final Path path = dfs.makeQualified(new Path(splitAttributes.getPath()));
    final FileSplit split = new FileSplit(path, splitAttributes.getStart(), splitAttributes.getLength(), new String[]{""});
    return new SequenceFileRecordReader(context, split, dfs);
  }

  @Override
  public int getReaderOperatorType() {
    return 4001;
  }

  @Override
  public RecordWriter getRecordWriter(OperatorContext context, EasyWriter writer) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getWriterOperatorType() {
    throw new UnsupportedOperationException();
  }
}
