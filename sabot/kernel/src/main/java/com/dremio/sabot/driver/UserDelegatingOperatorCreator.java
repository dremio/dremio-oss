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
package com.dremio.sabot.driver;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.spi.BatchStreamProvider;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.spi.TerminalOperator;

/**
 * Delegates the creation of each operator to the operator's config username.
 */
public class UserDelegatingOperatorCreator implements OperatorCreator {

  private final OperatorCreator delegate;
  private final String queryUser;

  public UserDelegatingOperatorCreator(String queryUser, OperatorCreator delegate) {
    super();
    this.delegate = delegate;
    this.queryUser = queryUser;
  }

  @Override
  public <T extends PhysicalOperator> SingleInputOperator getSingleInputOperator(final OperatorContext context, final T operator) throws Exception {
    final UserGroupInformation proxyUgi = ImpersonationUtil.createProxyUgi(operator.getUserName(), queryUser);
    return proxyUgi.doAs(new PrivilegedExceptionAction<SingleInputOperator>() {
      @Override
      public SingleInputOperator run() throws Exception {
        return delegate.getSingleInputOperator(context, operator);
      }
    });
  }

  @Override
  public <T extends PhysicalOperator> DualInputOperator getDualInputOperator(final OperatorContext context, final T operator) throws Exception {
    final UserGroupInformation proxyUgi = ImpersonationUtil.createProxyUgi(operator.getUserName(), queryUser);
    return proxyUgi.doAs(new PrivilegedExceptionAction<DualInputOperator>() {
      @Override
      public DualInputOperator run() throws Exception {
        return delegate.getDualInputOperator(context, operator);
      }
    });
  }

  @Override
  public <T extends PhysicalOperator> TerminalOperator getTerminalOperator(
      final TunnelProvider provider,
      final OperatorContext context,
      final T operator) throws Exception {
    final UserGroupInformation proxyUgi = ImpersonationUtil.createProxyUgi(operator.getUserName(), queryUser);
    return proxyUgi.doAs(new PrivilegedExceptionAction<TerminalOperator>() {
      @Override
      public TerminalOperator run() throws Exception {
        return delegate.getTerminalOperator(provider, context, operator);
      }
    });
  }

  @Override
  public <T extends PhysicalOperator> ProducerOperator getProducerOperator(final FragmentExecutionContext fec, final OperatorContext context, final T operator) throws Exception {
    final UserGroupInformation proxyUgi = ImpersonationUtil.createProxyUgi(operator.getUserName(), queryUser);
    return proxyUgi.doAs(new PrivilegedExceptionAction<ProducerOperator>() {
      @Override
      public ProducerOperator run() throws Exception {
        return delegate.getProducerOperator(fec, context, operator);
      }
    });
  }

  @Override
  public <T extends PhysicalOperator> ProducerOperator getReceiverOperator(
      final BatchStreamProvider buffers,
      final OperatorContext context,
      final T operator) throws Exception {
    final UserGroupInformation proxyUgi = ImpersonationUtil.createProxyUgi(operator.getUserName(), queryUser);
    return proxyUgi.doAs(new PrivilegedExceptionAction<ProducerOperator>() {
      @Override
      public ProducerOperator run() throws Exception {
        return delegate.getReceiverOperator(buffers, context, operator);
      }
    });
  }

}
