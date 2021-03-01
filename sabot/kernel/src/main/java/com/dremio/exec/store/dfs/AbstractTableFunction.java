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

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.tablefunction.TableFunction;

/**
 * Common base class for table functions
 */
public abstract class AbstractTableFunction implements TableFunction {
  protected VectorAccessible incoming;
  protected VectorContainer outgoing;
  protected final TableFunctionConfig functionConfig;
  protected final OperatorContext context;


  public AbstractTableFunction(OperatorContext context,
                               TableFunctionConfig functionConfig) {
    this.context = context;
    this.functionConfig = functionConfig;
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    this.incoming = accessible;
    this.outgoing = context.createOutputVectorContainer();
    BatchSchema outSchema = functionConfig.getOutputSchema();
    outgoing.addSchema(outSchema);
    outgoing.buildSchema();
    return outgoing;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(outgoing);
  }
}
