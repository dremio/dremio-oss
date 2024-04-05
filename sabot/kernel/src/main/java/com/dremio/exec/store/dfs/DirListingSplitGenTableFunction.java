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
package com.dremio.exec.store.dfs;

import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;

/**
 * Table function converts input data file path, partition information and file size, and generates
 * a VarBinary, which contains serialised {@link com.dremio.exec.store.SplitAndPartitionInfo}.
 *
 * <p>This class extends {@link SplitGenTableFunction} to override the expected input to match the
 * output of {@link com.dremio.exec.store.metadatarefresh.dirlisting.DirListingScanPrel}.
 */
public class DirListingSplitGenTableFunction extends SplitGenTableFunction {

  public DirListingSplitGenTableFunction(
      FragmentExecutionContext fec, OperatorContext context, TableFunctionConfig functionConfig) {
    super(fec, context, functionConfig);
  }

  @Override
  protected void setCurrentPath(int row) {
    currentPath = new String(pathVector.get(row), StandardCharsets.UTF_8);
  }

  @Override
  protected void initializeIncomingVectors() {
    pathVector =
        (VarCharVector)
            VectorUtil.getVectorFromSchemaPath(
                incoming, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);
    sizeVector =
        (BigIntVector)
            VectorUtil.getVectorFromSchemaPath(
                incoming, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_SIZE);
    modTimeVector =
        (BigIntVector)
            VectorUtil.getVectorFromSchemaPath(
                incoming, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.MODIFICATION_TIME);
  }
}
