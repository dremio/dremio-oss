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
package com.dremio.exec.store.hive.exec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import com.dremio.common.util.Closeable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.hive.HivePf4jPlugin;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingRecordReader;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;

public class HiveDirListingRecordReader extends DirListingRecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveDirListingRecordReader.class);

  public HiveDirListingRecordReader(OperatorContext context, FileSystem fs, DirListInputSplitProto.DirListInputSplit dirListInputSplit, boolean isRecursive,
                                    BatchSchema tableSchema, List<PartitionProtobuf.PartitionValue> partitionValues, boolean discoverPartitions) {
    super(context, fs, dirListInputSplit, isRecursive, tableSchema, partitionValues, discoverPartitions, false);
  }

  @Override
  protected int iterateDirectory() throws IOException {
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      return super.iterateDirectory();
    }
  }

  @Override
  protected void initDirIterator(boolean isFile) throws IOException {
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      super.initDirIterator(false);
    } catch (FileNotFoundException e) {
      logger.debug("FNF error while listing directory " + operatingPath, e);
    }
  }

}
