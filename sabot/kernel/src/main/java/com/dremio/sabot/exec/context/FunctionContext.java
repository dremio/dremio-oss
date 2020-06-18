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
package com.dremio.sabot.exec.context;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.arrow.vector.types.Types.MinorType;

import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.store.PartitionExplorer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

/**
 * Defines the query state and shared resources available to Functions through
 * injectables. For use in a function, include a {@link javax.inject.Inject}
 * annotation on a UDF class member with any of the types available through
 * this interface.
 */
public interface FunctionContext {

  // Map between injectable classes and their respective getter methods
  // used for code generation
  public static final ImmutableMap<Class<?>, String> INJECTABLE_GETTER_METHODS =
      new ImmutableMap.Builder<Class<?>, String>()
          .put(ArrowBuf.class, "getManagedBuffer")
          .put(BufferManager.class, "getBufferManager")
          .put(PartitionExplorer.class, "getPartitionExplorer")
          .put(ContextInformation.class, "getContextInformation")
          .put(FunctionErrorContext.class, "getFunctionErrorContext")
          .build();


  /**
   * Get the context information such as:
   *  - query start time
   *  - root fragment timezone
   *  - query userName
   *  - system userName
   *  - default schema name in current session at the time of query.
   *
   * @return - ContextInformation
   */
  ContextInformation getContextInformation();

  /**
   * For UDFs to allocate general purpose intermediate buffers we provide the
   * ArrowBuf type as an injectable, which provides access to an off-heap
   * buffer that can be tracked by Dremio and re-allocated as needed.
   *
   * @return - a buffer managed by Dremio, connected to the fragment allocator
   *           for memory management
   */
  ArrowBuf getManagedBuffer();

  /**
   * For UDFs
   */
  BufferManager getBufferManager();

  /**
   * A partition explorer allows UDFs to view the sub-partitions below a
   * particular partition. This allows for the implementation of UDFs to
   * query against the partition information, without having to read
   * the actual data contained in the partition. This interface is designed
   * for UDFs that take only constant inputs, as this interface will only
   * be useful if we can evaluate the constant UDF at planning time.
   *
   * Any function defined to use this interface that is not evaluated
   * at planning time by the constant folding rule will be querying
   * the storage plugin for meta-data for each record processed.
   *
   * Be sure to check the query plans to see that this expression has already
   * been evaluated during planning if you write UDFs against this interface.
   *
   * See {@link com.dremio.exec.expr.fn.impl.DirectoryExplorers} for
   * example usages of this interface.
   *
   * @return - an object for exploring partitions of all available schemas
   */
  PartitionExplorer getPartitionExplorer();

  /**
   * Register a function error context for later use (as an injectable) in a function
   * @return an ID that can be used as a handle to retrieve the function error context from generated code
   */
  int registerFunctionErrorContext(FunctionErrorContext errorContext);

  /**
   * The FunctionErrorContext is provided as an injectable to all functions that need to return an error
   * This object allows the error message to identify the context of the function within the wider query
   * @param errorContextId -- the ID that was returned by a previous call to {@link #registerFunctionErrorContext}
   *
   * @return an object that provides the context for error messages
   */
  FunctionErrorContext getFunctionErrorContext(int errorContextId);

  /**
   * Temporary hack: until Phase3 of the generalized function exception framework
   * TODO (DX-9622)
   */
  FunctionErrorContext getFunctionErrorContext();

  /**
   * Get Compilation options
   * @return compilation options.
   */
  CompilationOptions getCompilationOptions();

  /**
   * Works with value holders cache which holds constant value and its wrapper by type.
   * If value is absent uses holderInitializer to create holder and adds it to cache.
   *
   * @return - a wrapper object for an constant value.
   */
  ValueHolder getConstantValueHolder(String value, MinorType type, Function<ArrowBuf, ValueHolder> holderInitializer);
}
