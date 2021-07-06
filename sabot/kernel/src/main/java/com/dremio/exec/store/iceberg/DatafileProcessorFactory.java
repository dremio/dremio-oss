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
package com.dremio.exec.store.iceberg;

import java.util.concurrent.Callable;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.store.BlockBasedSplitGenerator;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.cache.BlockLocationsCacheManager;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;

/**
 * DatafileProcessorFactory : For input arguments this returns datafile processor
 */
public class DatafileProcessorFactory {
  private final FragmentExecutionContext fec;
  private final OpProps props;
  private final OperatorContext context;

  public DatafileProcessorFactory(FragmentExecutionContext fec, OpProps props, OperatorContext context) {
    this.fec = fec;
    this.props = props;
    this.context = context;
  }

  DatafileProcessor getDatafileProcessor(TableFunctionConfig functionConfig, BlockBasedSplitGenerator.SplitCreator splitCreator) {
    if (functionConfig.getType() == TableFunctionConfig.FunctionType.METADATA_REFRESH_MANIFEST_SCAN) {
      return new PathGeneratingDatafileProcessor();
    } else {
      String pluginId = functionConfig.getFunctionContext().getPluginId().getConfig().getId().getId();
      StoragePlugin plugin = wrap(() -> fec.getStoragePlugin(functionConfig.getFunctionContext().getPluginId()));
      BlockLocationsCacheManager cacheManager = null;
      if (plugin instanceof FileSystemPlugin) {
        FileSystem fs = wrap(() -> ((FileSystemPlugin<?>) plugin).createFS(props.getUserName(), context));
        cacheManager = BlockLocationsCacheManager.newInstance(fs, pluginId, context);
      }
      /*else {
        TODO: DX-31834 : For plugins other than FileSystemPlugin, find a way to get FileSystem instance and create cacheManager
      }*/
      return new SplitGeneratingDatafileProcessor(cacheManager, context, functionConfig.getFunctionContext(), splitCreator);
    }
  }

  private <T> T wrap(Callable<T> task) {
    try {
      return task.call();
    } catch (Exception e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }
}
