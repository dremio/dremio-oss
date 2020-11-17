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
package com.dremio.exec;

import static org.mockito.Mockito.when;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import com.dremio.common.AutoCloseables;
import com.dremio.common.JULBridge;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.expr.fn.DecimalFunctionImplementationRegistry;
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
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.server.options.QueryOptionManager;
import com.dremio.exec.server.options.SessionOptionManager;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserRPCServer.UserClientConnection;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.telemetry.api.metrics.Metrics;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public class ExecTest extends DremioTest {

  private BufferAllocator rootAllocator;
  protected BufferAllocator allocator;

  private static volatile FunctionImplementationRegistry FUNCTION_REGISTRY;
  private static volatile FunctionImplementationRegistry FUNCTION_REGISTRY_DECIMAL;
  private static final OptionManager OPTION_MANAGER = Mockito.mock(OptionManager.class);

  protected static FunctionImplementationRegistry FUNCTIONS( ){
    // initialize once so avoid having to regenerate functions repetitvely in tests. So so lazily so tests that don't need, don't do.
    if(FUNCTION_REGISTRY == null){
      FUNCTION_REGISTRY = new FunctionImplementationRegistry(DEFAULT_SABOT_CONFIG,
        CLASSPATH_SCAN_RESULT, OPTION_MANAGER);
    }
    return FUNCTION_REGISTRY;
  }

  protected static boolean isComplexTypeSupport() {
    return PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal();
  }

  protected static FunctionImplementationRegistry DECIMAL_FUNCTIONS( ){
    if(FUNCTION_REGISTRY_DECIMAL == null){
      FUNCTION_REGISTRY_DECIMAL = new DecimalFunctionImplementationRegistry(DEFAULT_SABOT_CONFIG,
        CLASSPATH_SCAN_RESULT, OPTION_MANAGER);
    }
    return FUNCTION_REGISTRY_DECIMAL;
  }

  static {
    JULBridge.configure();
  }

  @Before
  public void initAllocators() {
    rootAllocator = RootAllocatorFactory.newRoot(DEFAULT_SABOT_CONFIG);
    allocator = rootAllocator.newChildAllocator(TEST_NAME.getMethodName(), 0, rootAllocator.getLimit());
  }

  @After
  public void clear(){
    // TODO:  (Re DRILL-1735) Check whether still needed now that
    // BootstrapContext.close() resets the metrics.
    Metrics.resetMetrics();
    AutoCloseables.closeNoChecked(allocator);
    AutoCloseables.closeNoChecked(rootAllocator);
  }

  protected BufferAllocator getAllocator() {
    return allocator;
  }

  protected QueryContext mockQueryContext(SabotContext dbContext) throws Exception {
    final SessionOptionManager sessionOptionManager = new SessionOptionManagerImpl(dbContext.getOptionValidatorListing());

    final UserSession userSession = UserSession.Builder.newBuilder()
      .withSessionOptionManager(sessionOptionManager, dbContext.getOptionManager())
      .build();
    final OptionManager queryOptions = OptionManagerWrapper.Builder.newBuilder()
      .withOptionManager(userSession.getOptions())
      .withOptionManager(new QueryOptionManager(userSession.getOptions().getOptionValidatorListing()))
      .build();
    final ExecutionControls executionControls = new ExecutionControls(queryOptions, NodeEndpoint.getDefaultInstance());
    FunctionImplementationRegistry functions = queryOptions.getOption(PlannerSettings
      .ENABLE_DECIMAL_V2) ? DECIMAL_FUNCTIONS() : FUNCTIONS();
    final OperatorTable table = new OperatorTable(functions);
    final LogicalPlanPersistence lp = dbContext.getLpPersistence();
    final CatalogService registry = dbContext.getCatalogService();

    final QueryContext context = Mockito.mock(QueryContext.class);
    when(context.getSession()).thenReturn(userSession);
    when(context.getLpPersistence()).thenReturn(lp);
    when(context.getCatalogService()).thenReturn(registry);
    when(context.getFunctionRegistry()).thenReturn(functions);
    when(context.getSession()).thenReturn(UserSession.Builder.newBuilder()
      .withSessionOptionManager(sessionOptionManager, dbContext.getOptionManager())
      .setSupportComplexTypes(true).build());
    when(context.getCurrentEndpoint()).thenReturn(NodeEndpoint.getDefaultInstance());
    when(context.getActiveEndpoints()).thenReturn(ImmutableList.of(NodeEndpoint.getDefaultInstance()));
    when(context.getPlannerSettings()).thenReturn(new PlannerSettings(dbContext.getConfig(), queryOptions,
      () -> dbContext.getClusterResourceInformation()));
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
