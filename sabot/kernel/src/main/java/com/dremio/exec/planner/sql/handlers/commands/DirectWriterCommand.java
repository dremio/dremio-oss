/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.sql.handlers.commands;

import static com.dremio.exec.planner.physical.PlannerSettings.QUERY_RESULTS_STORE_TABLE;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.text.StrTokenizer;

import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.ParserConfig;
import com.dremio.exec.planner.sql.handlers.direct.SqlDirectHandler;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.WritableBatch;
import com.dremio.exec.server.NodeDebugContextProvider;
import com.dremio.exec.store.easy.arrow.ArrowFormatPlugin;
import com.dremio.exec.store.pojo.PojoDataType;
import com.dremio.exec.store.pojo.PojoRecordReader;
import com.dremio.options.OptionManager;
import com.dremio.sabot.driver.OperatorCreatorRegistry;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.VectorContainerMutator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.spi.SingleInputOperator.State;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableMap;

/**
 * Used when we want to record the results of a direct query.
 */
public class DirectWriterCommand<T> implements CommandRunner<Object> {

  private final SqlNode sqlNode;
  private final AttemptObserver observer;
  private final SqlDirectHandler<T> handler;
  private final QueryContext context;
  private final String sql;

  private List<T> result;

  public DirectWriterCommand(String sql, QueryContext context, SqlNode sqlNode,  SqlDirectHandler<T> handler, AttemptObserver observer) {
    super();
    this.handler = handler;
    this.context = context;
    this.sqlNode = sqlNode;
    this.observer = observer;
    this.sql = sql;
  }

  @Override
  public double plan() throws Exception {
    observer.planStart(sql);
    observer.planValidated(new PojoDataType(handler.getResultType()).getRowType(JavaTypeFactoryImpl.INSTANCE), null, 0);
    result = handler.toResult(sql, sqlNode);
    observer.planCompleted(null);
    return 1;
  }

  @Override
  public void close() throws Exception {
    // no-op
  }

  @Override
  public Object execute() throws Exception {

    observer.execStarted(null);
    final BatchSchema schema = PojoRecordReader.getSchema(handler.getResultType());
    final CollectingOutcomeListener listener = new CollectingOutcomeListener();
    Writer writer = getWriter(context.getOptions());
    //TODO: non-trivial expense. Move elsewhere.
    OperatorCreatorRegistry registry = new OperatorCreatorRegistry(context.getScanResult());
    OperatorContextImpl ocx = createContext(writer);
    try(
        OperatorContextImpl oc = ocx;
        BufferAllocator allocator = context.getAllocator().newChildAllocator("direct-writer", 0, Long.MAX_VALUE);
        VectorContainer vc = VectorContainer.create(allocator, schema);
        BufferManager manager = new BufferManagerImpl(allocator);
        final PojoRecordReader<T> reader = new PojoRecordReader<>(handler.getResultType(), result.iterator());
        SingleInputOperator op = registry.getSingleInputOperator(ocx, writer);
        ) {
      reader.setup(new VectorContainerMutator(vc, manager));
      VectorAccessible output = op.setup(vc);
      int count = 0;
      while( (count = reader.next()) != 0){
        vc.setRecordCount(count);
        op.consumeData(count);
        depleteSend(op, output, listener);
      }
      op.noMoreToConsume();
      depleteSend(op, output, listener);

      listener.waitForFinish();
    }

    return null;
  }

  private void depleteSend(SingleInputOperator op, VectorAccessible output, CollectingOutcomeListener listener) throws Exception{
    while(op.getState() == State.CAN_PRODUCE){
      int count = op.outputData();
      final WritableBatch w = WritableBatch.get(output);
      QueryData header = QueryData.newBuilder() //
          .setQueryId(context.getQueryId()) //
          .setRowCount(count) //
          .setDef(w.getDef()).build();
      QueryWritableBatch batch = new QueryWritableBatch(header, w.getBuffers());
      listener.increment();
      observer.execDataArrived(listener, batch);
    }
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.SYNC_QUERY;
  }

  @Override
  public String getDescription() {
    return "execute and store; direct";
  }

  private Writer getWriter(OptionManager options) throws IOException{
    final String storeTablePath = options.getOption(QUERY_RESULTS_STORE_TABLE.getOptionName()).getStringVal();
    final List<String> storeTable = new StrTokenizer(storeTablePath, '.', ParserConfig.QUOTING.string.charAt(0))
        .setIgnoreEmptyTokens(true).getTokenList();

    // Query results are stored in arrow format. If need arises, we can change
    // this to a configuration option.
    final Map<String, Object> storageOptions = ImmutableMap.<String, Object> of("type", ArrowFormatPlugin.ARROW_DEFAULT_NAME);

    final CreateTableEntry createTableEntry = context.getCatalog()
        .resolveCatalog(SystemUser.SYSTEM_USERNAME)
        .createNewTable(new NamespaceKey(storeTable), WriterOptions.DEFAULT, storageOptions);
    return createTableEntry.getWriter(null);
  }


  @Override
  public String toString() {
    return "DirectWriterCommand [handler=" + handler + ", sql=" + sql + "]";
  }

  private OperatorContextImpl createContext(Writer writer) {
    BufferAllocator allocator = context.getAllocator().newChildAllocator("direct-command", 0, Long.MAX_VALUE);
    final OperatorStats stats = new OperatorStats(new OpProfileDef(0,0,0), allocator);
    final OperatorContextImpl oc = new OperatorContextImpl(
        context.getConfig(),
        FragmentHandle.newBuilder().setQueryId(context.getQueryId()).setMajorFragmentId(0).setMinorFragmentId(0).build(),
        writer,
        allocator,
        allocator,
        null,
        stats,
        null,
        null,
        context.getFunctionRegistry(),
        null,
        context.getOptions(),
        context.getNamespaceService(),
        NodeDebugContextProvider.NOOP,
        60000);
    return oc;
  }
}
