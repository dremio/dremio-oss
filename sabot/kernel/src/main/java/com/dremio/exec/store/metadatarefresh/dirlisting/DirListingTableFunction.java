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
package com.dremio.exec.store.metadatarefresh.dirlisting;

import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.DirListingTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.MutatorSchemaChangeCallBack;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.arrow.vector.VarCharVector;

/** Table function for Directory listing scan. */
public class DirListingTableFunction extends AbstractTableFunction {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(DirListingTableFunction.class);
  private final DirListingTableFunctionContext dirListingContext;
  private final OpProps props;

  private final SupportsInternalIcebergTable plugin;
  private VarCharVector inputPath;
  private DirListingRecordReader dirListingRecordReader;
  private ScanOperator.ScanMutator mutator;
  private MutatorSchemaChangeCallBack callBack = new MutatorSchemaChangeCallBack();

  public DirListingTableFunction(
      FragmentExecutionContext fragmentExecutionContext,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig)
      throws ExecutionSetupException {
    super(context, functionConfig);
    this.props = props;

    dirListingContext = (DirListingTableFunctionContext) functionConfig.getFunctionContext();
    plugin =
        fragmentExecutionContext.getStoragePlugin(
            functionConfig.getFunctionContext().getPluginId());
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);

    inputPath = (VarCharVector) getVectorFromSchemaPath(incoming, SystemSchemas.PATH);

    VectorContainer outgoing = (VectorContainer) super.setup(incoming);
    this.mutator = new ScanOperator.ScanMutator(outgoing, context, callBack);
    this.mutator.allocate();

    return outgoing;
  }

  @Override
  public void startRow(int row) throws Exception {
    String pathWithScheme = new String(inputPath.get(row), StandardCharsets.UTF_8);
    String path = Path.getContainerSpecificRelativePath(Path.of(pathWithScheme));

    DirListInputSplitProto.DirListInputSplit dirListInputSplit =
        DirListInputSplitProto.DirListInputSplit.newBuilder()
            .setOperatingPath(path)
            .setRootPath(path)
            .setHasVersion(dirListingContext.hasVersion())
            .setReadSignature(Long.MAX_VALUE)
            .build();

    FileSystem fs;
    try {
      fs = plugin.createFSWithAsyncOptions(pathWithScheme, props.getUserName(), context);
    } catch (IOException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }

    dirListingRecordReader =
        plugin.createDirListRecordReader(
            context,
            fs,
            dirListInputSplit,
            dirListingContext.allowRecursiveListing(),
            functionConfig.getTableSchema(),
            Collections.emptyList());

    dirListingRecordReader.setup(mutator);
  }

  @Override
  public int processRow(int startOutIndex, int maxIndex) throws Exception {
    int outputCount = dirListingRecordReader.nextBatch(startOutIndex, startOutIndex + maxIndex);
    int totalOutputCount = startOutIndex + outputCount;
    outgoing.forEach(vw -> vw.getValueVector().setValueCount(totalOutputCount));
    outgoing.setRecordCount(totalOutputCount);
    return outputCount;
  }

  @Override
  public void closeRow() throws Exception {
    dirListingRecordReader.close();
    dirListingRecordReader = null;
  }
}
