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
package com.dremio.exec.store.avro;

import java.io.IOException;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.file.proto.EasyDatasetSplitXAttr;
import com.google.common.collect.Lists;

/**
 * Format plugin for Avro data files.
 */
public class AvroFormatPlugin extends EasyFormatPlugin<AvroFormatConfig> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvroFormatPlugin.class);

  public AvroFormatPlugin(String name, SabotContext context, StoragePluginConfig storagePluginConfig, FileSystemPlugin fsPlugin) {
    this(name, context, storagePluginConfig, new AvroFormatConfig(), fsPlugin);
  }

  public AvroFormatPlugin(String name, SabotContext context, StoragePluginConfig config, AvroFormatConfig formatPluginConfig, FileSystemPlugin fsPlugin) {
    super(name, context, config, formatPluginConfig, true, false, true, false, Lists.newArrayList("avro"), "avro", fsPlugin);
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context, FileSystemWrapper dfs, EasyDatasetSplitXAttr splitAttributes, List<SchemaPath> columns) throws ExecutionSetupException {
    return new AvroRecordReader(context, splitAttributes.getPath(), splitAttributes.getStart(), splitAttributes.getLength(), columns, getFsPlugin().getFsConf());
  }

  @Override
  public RecordWriter getRecordWriter(OperatorContext context, EasyWriter writer) throws IOException {
    throw UserException
    .unsupportedError()
    .message("Writing output in Avro format is not supported")
    .build(logger);
  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.AVRO_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    throw new UnsupportedOperationException("unimplemented");
  }


}
