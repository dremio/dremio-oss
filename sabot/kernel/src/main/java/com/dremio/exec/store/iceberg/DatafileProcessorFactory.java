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

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
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

  DatafileProcessor getDatafileProcessor(TableFunctionConfig functionConfig) {
    switch (functionConfig.getType()) {
      case METADATA_REFRESH_MANIFEST_SCAN:
        return new PathGeneratingDatafileProcessor();
      case SPLIT_GEN_MANIFEST_SCAN:
        SupportsInternalIcebergTable plugin = IcebergUtils.getSupportsInternalIcebergTablePlugin(fec, functionConfig.getFunctionContext().getPluginId());
        return new SplitGeneratingDatafileProcessor(context, plugin, props, functionConfig.getFunctionContext());
      default:
        throw new UnsupportedOperationException("Unknown table function type " + functionConfig.getType());
    }

  }
}
