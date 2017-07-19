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
package com.dremio.sabot.exec.context;

import io.netty.buffer.ArrowBuf;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.compile.CodeCompiler;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.ClassProducerImpl;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

@VisibleForTesting
public class OperatorContextImpl extends OperatorContext implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorContextImpl.class);

  private final SabotConfig config;
  private final FragmentHandle handle;
  private final BufferAllocator allocator;
  private final ExecutionControls executionControls;
  private boolean closed = false;
  private final PhysicalOperator popConfig;
  private final OperatorStats stats;
  private final BufferManager manager;
  private final CodeCompiler compiler;
  private FileSystemWrapper fs;
  private final ExecutorService executor;

  private final FunctionLookupContext functions;
  private final ClassProducer producer;
  private final ContextInformation contextInformation;
  private final OptionManager optionManager;
  private final int targetBatchSize;
  private final NamespaceService ns;

  /**
   * This lazily initialized executor service is used to submit a {@link Callable task} that needs a proxy user. There
   * is no pool that is created; this pool is a decorator around {@link ForemenWorkManager#executor the worker pool} that
   * returns a {@link ListenableFuture future} for every task that is submitted. For the shutdown sequence,
   * see {@link ForemenWorkManager#close}.
   */
  private ListeningExecutorService delegatePool;

  public OperatorContextImpl(
      SabotConfig config,
      FragmentHandle handle,
      PhysicalOperator popConfig,
      BufferAllocator allocator,
      CodeCompiler compiler,
      OperatorStats stats,
      ExecutionControls executionControls,
      ExecutorService executor,
      FunctionLookupContext functions,
      ContextInformation contextInformation,
      OptionManager optionManager,
      NamespaceService namespaceService,
      int targetBatchSize) throws OutOfMemoryException {
    this.config = config;
    this.handle = handle;
    this.allocator = allocator;
    this.popConfig = popConfig;
    this.manager = new BufferManagerImpl(allocator);
    this.compiler = compiler;
    this.stats = stats;
    this.executionControls = executionControls;
    this.executor = executor;
    this.functions = functions;
    this.contextInformation = contextInformation;
    this.optionManager = optionManager;
    this.targetBatchSize = targetBatchSize;
    this.ns = namespaceService;

    this.producer = new ClassProducerImpl(compiler, functions, contextInformation, manager);
  }

  public OperatorContextImpl(
      SabotConfig config,
      BufferAllocator allocator,
      OptionManager optionManager,
      int targetBatchSize
      ) {
    this(config, null, null, allocator, null, null, null, null, null, null, optionManager, null, targetBatchSize);
  }


  public SabotConfig getConfig(){
    return config;
  }

  public ArrowBuf replace(ArrowBuf old, int newSize) {
    return manager.replace(old, newSize);
  }

  public ArrowBuf getManagedBuffer() {
    return manager.getManagedBuffer();
  }

  public ArrowBuf getManagedBuffer(int size) {
    return manager.getManagedBuffer(size);
  }

  public ExecutionControls getExecutionControls() {
    return executionControls;
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  public BufferAllocator getAllocator() {
    if (allocator == null) {
      throw new UnsupportedOperationException("SqlOperatorImpl context does not have an allocator");
    }
    return allocator;
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public int getTargetBatchSize() {
    return targetBatchSize;
  }

  @Override
  public void close() throws Exception {
    if (closed) {
      logger.warn("Attempted to close SqlOperatorImpl context for {}, but context is already closed", popConfig != null ? popConfig.getClass().getName() : "<unknown>");
      return;
    }

    try{
      AutoCloseables.close(manager, allocator, fs);
    }finally{
      closed = true;
    }
  }

  public OperatorStats getStats() {
    return stats;
  }

  public <RESULT> ListenableFuture<RESULT> runCallableAs(final UserGroupInformation proxyUgi,
                                                         final Callable<RESULT> callable) {
    synchronized (this) {
      if (delegatePool == null) {
        delegatePool = MoreExecutors.listeningDecorator(executor);
      }
    }
    return delegatePool.submit(new Callable<RESULT>() {
      @Override
      public RESULT call() throws Exception {
        final Thread currentThread = Thread.currentThread();
        final String originalThreadName = currentThread.getName();
        currentThread.setName(proxyUgi.getUserName() + ":task-delegate-thread");
        final RESULT result;
        try {
          result = proxyUgi.doAs(new PrivilegedExceptionAction<RESULT>() {
            @Override
            public RESULT run() throws Exception {
              return callable.call();
            }
          });
        } finally {
          currentThread.setName(originalThreadName);
        }
        return result;
      }
    });
  }

  public OptionManager getOptions() {
    return optionManager;
  }

  @Override
  public FragmentHandle getFragmentHandle() {
    return handle;
  }

  public FunctionContext getFunctionContext() {
    return producer.getFunctionContext();
  }

  @Override
  public FileSystemWrapper newFileSystem(Configuration conf) throws IOException {
    Preconditions.checkState(fs == null, "Tried to create a second FileSystem. Can only be called once per OperatorContext");
    fs = new FileSystemWrapper(conf, getStats());
    return fs;
  }

  public ClassProducer getClassProducer(){
    return producer;
  }

  @Override
  public NamespaceService getNamespaceService() {
    return ns;
  }

}
