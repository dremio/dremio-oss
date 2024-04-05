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
package com.dremio.exec.planner.sql.handlers.commands;

import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.handlers.direct.SqlDirectHandler;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.WritableBatch;
import com.dremio.exec.store.pojo.PojoDataType;
import com.dremio.exec.store.pojo.PojoRecordReader;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.dremio.sabot.op.scan.VectorContainerMutator;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.calcite.sql.SqlNode;

public class DirectCommand<T, R> implements CommandRunner<Object> {

  private final SqlNode sqlNode;
  private final AttemptObserver observer;
  private final SqlDirectHandler<T> handler;
  private final QueryContext context;
  private final String sql;

  private List<T> result;

  public DirectCommand(
      String sql,
      QueryContext context,
      SqlNode sqlNode,
      SqlDirectHandler<T> handler,
      AttemptObserver observer) {
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
    observer.planValidated(
        new PojoDataType(handler.getResultType()).getRowType(JavaTypeFactoryImpl.INSTANCE),
        sqlNode,
        0,
        true);
    result = handler.toResult(sql, sqlNode);
    observer.planCompleted(null, PojoRecordReader.getSchema(handler.getResultType()));
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

    try (BufferAllocator allocator =
            context.getAllocator().newChildAllocator("direct-command", 0, Long.MAX_VALUE);
        VectorContainer vc = VectorContainer.create(allocator, schema);
        BufferManager manager = new BufferManagerImpl(allocator);
        final PojoRecordReader<T> reader =
            new PojoRecordReader<>(handler.getResultType(), result.iterator()); ) {

      reader.setup(new VectorContainerMutator(vc, manager));
      int count = 0;
      while ((count = reader.next()) != 0) {
        vc.setRecordCount(count);
        final WritableBatch w = WritableBatch.get(vc);
        QueryData header =
            QueryData.newBuilder() //
                .setQueryId(context.getQueryId()) //
                .setRowCount(count) //
                .setDef(w.getDef())
                .build();
        QueryWritableBatch batch = new QueryWritableBatch(header, w.getBuffers());

        listener.increment();
        observer.execDataArrived(listener, batch);
      }

      listener.waitForFinish();
    }

    return null;
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.SYNC_QUERY;
  }

  @Override
  public String getDescription() {
    return "execute; direct";
  }
}
