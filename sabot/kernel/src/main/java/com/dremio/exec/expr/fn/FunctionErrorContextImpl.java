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
package com.dremio.exec.expr.fn;

import com.dremio.common.exceptions.UserException;
import com.google.errorprone.annotations.FormatMethod;

/**
 * Implementation of the FunctionErrorContext interface
 */
public class FunctionErrorContextImpl implements FunctionErrorContext {
  public static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionErrorContextImpl.class);

  private int id;  // ID by which the error context is registered within a FunctionContext. -1 == unassigned

  /**
   * Creation only exposed through {@link FunctionErrorContextBuilder}
   */
  FunctionErrorContextImpl() {
    this.id = -1;  // Meaning; unassigned
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public void setId(int id) {
    this.id = id;
  }

  @Override
  public ExceptionBuilder error() {
    return addFullContext(new ExceptionBuilderImpl());
  }

  @Override
  public ExceptionBuilder error(final Throwable cause) {
    return addFullContext(new ExceptionBuilderImpl(cause));
  }

  private ExceptionBuilderImpl addFullContext(ExceptionBuilderImpl b) {
    // TODO(Vanco): add context: where in the query is this error?
    return b;
  }

  private static class ExceptionBuilderImpl implements FunctionErrorContext.ExceptionBuilder {
    private UserException.Builder b;

    ExceptionBuilderImpl() {
      b = UserException.functionError();
    }

    ExceptionBuilderImpl(final Throwable cause) {
      b = UserException.functionError(cause);
    }

    public ExceptionBuilder message(final String message) {
      b.message(message);
      return this;
    }

    @FormatMethod
    public ExceptionBuilder message(final String format, final Object... args) {
      b.message(format, args);
      return this;
    }

    public ExceptionBuilder addContext(final String value) {
      b.addContext(value);
      return this;
    }

    @FormatMethod
    public ExceptionBuilder addContext(final String value, Object... args) {
      b.addContext(value, args);
      return this;
    }

    public ExceptionBuilder addContext(final String name, final String value) {
      b.addContext(name, value);
      return this;
    }

    /**
     * Builds an exception that can be thrown by the caller
     */
    public RuntimeException build() {
      return b.build(logger);
    }
  }
}
