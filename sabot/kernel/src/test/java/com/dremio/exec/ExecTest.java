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
package com.dremio.exec;

import static org.mockito.Mockito.when;

import java.util.logging.LogManager;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.After;
import org.mockito.Mockito;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.QueryResult;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.options.QueryOptionManager;
import com.dremio.exec.server.options.SessionOptionManager;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.metrics.Metrics;
import com.dremio.sabot.op.screen.QueryWritableBatch;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.sabot.rpc.user.UserRPCServer.UserClientConnection;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public class ExecTest extends DremioTest {

  protected final BufferAllocator allocator = RootAllocatorFactory.newRoot(DEFAULT_SABOT_CONFIG);

  private static volatile FunctionImplementationRegistry FUNCTION_REGISTRY;

  protected static FunctionImplementationRegistry FUNCTIONS( ){
    // initialize once so avoid having to regenerate functions repetitvely in tests. So so lazily so tests that don't need, don't do.
    if(FUNCTION_REGISTRY == null){
      FUNCTION_REGISTRY = new FunctionImplementationRegistry(DEFAULT_SABOT_CONFIG, CLASSPATH_SCAN_RESULT);
    }
    return FUNCTION_REGISTRY;
  }

  static {
    LogManager.getLogManager().reset();
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  @After
  public void clear(){
    // TODO:  (Re DRILL-1735) Check whether still needed now that
    // BootstrapContext.close() resets the metrics.
    Metrics.resetMetrics();
    allocator.close();
  }

  protected QueryContext mockQueryContext(SabotContext dbContext) throws Exception {
    final UserSession userSession = UserSession.Builder.newBuilder().withOptionManager(dbContext.getOptionManager()).build();
    final SessionOptionManager sessionOptions = (SessionOptionManager) userSession.getOptions();
    final QueryOptionManager queryOptions = new QueryOptionManager(sessionOptions);
    final ExecutionControls executionControls = new ExecutionControls(queryOptions, NodeEndpoint.getDefaultInstance());
    final OperatorTable table = new OperatorTable(FUNCTIONS());
    final SchemaPlus root = CalciteSchema.createRootSchema(false /* addMetadata */, false /* cache */).plus();
    final LogicalPlanPersistence lp = dbContext.getLpPersistence();
    final StoragePluginRegistry registry = dbContext.getStorage();

    final QueryContext context = Mockito.mock(QueryContext.class);
    when(context.getNewDefaultSchema()).thenReturn(root);
    when(context.getSession()).thenReturn(userSession);
    when(context.getLpPersistence()).thenReturn(lp);
    when(context.getStorage()).thenReturn(registry);
    when(context.getFunctionRegistry()).thenReturn(FUNCTIONS());
    when(context.getSession()).thenReturn(UserSession.Builder.newBuilder().setSupportComplexTypes(true).build());
    when(context.getCurrentEndpoint()).thenReturn(NodeEndpoint.getDefaultInstance());
    when(context.getActiveEndpoints()).thenReturn(ImmutableList.of(NodeEndpoint.getDefaultInstance()));
    when(context.getPlannerSettings()).thenReturn(new PlannerSettings(queryOptions, FUNCTIONS(), dbContext.getClusterResourceInformation()));
    when(context.getOptions()).thenReturn(queryOptions);
    when(context.getConfig()).thenReturn(DEFAULT_SABOT_CONFIG);
    when(context.getOperatorTable()).thenReturn(table);
    when(context.getAllocator()).thenReturn(allocator);
    when(context.getExecutionControls()).thenReturn(executionControls);
    when(context.getMaterializationProvider()).thenReturn(Mockito.mock(MaterializationDescriptorProvider.class));
    return context;
  }

  public static UserClientConnection mockUserClientConnection(QueryContext context){
    final UserSession session = context != null ? context.getSession() : Mockito.mock(UserSession.class);
    return new UserClientConnection(){

      @Override
      public void addTerminationListener(GenericFutureListener<? extends Future<? super Void>> listener) {
      }

      @Override
      public void removeTerminationListener(GenericFutureListener<? extends Future<? super Void>> listener) {
      }

      @Override
      public UserSession getSession() {
        return session;
      }

      @Override
      public void sendResult(RpcOutcomeListener<Ack> listener, QueryResult result) {
        listener.success(Acks.OK, null);
      }

      @Override
      public void sendData(RpcOutcomeListener<Ack> listener, QueryWritableBatch result) {
        try{
          AutoCloseables.close((AutoCloseable[]) result.getBuffers());
          listener.success(Acks.OK, null);
        }catch(Exception ex){
          listener.failed(new RpcException(ex));
        }
      }

    };
  }

}
