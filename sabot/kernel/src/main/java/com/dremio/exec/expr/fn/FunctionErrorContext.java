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
package com.dremio.exec.expr.fn;

import com.dremio.exec.record.BatchSchema;

/**
 * Injectable used in SimpleFunction definitions. Function code uses an object of this interface to construct exceptions
 * that have a full context -- in particular, where in the original expression did the function occur.
 */
public interface FunctionErrorContext {
  /**
   * @return the ID by which this FunctionErrorContext is registered within the enclosing FunctionContext
   */
  int getId();

  /**
   * @param id the ID by which this FunctionErrorContext is registered within the enclosing FunctionContext
   */
  void setId(int id);

  /**
   * @return a builder for an exception carrying the error context
   */
  ExceptionBuilder error();

  /**
   * @input the original exception cause that's being wrapped by more context
   * @return a builder for an exception carrying the error context
   */
  ExceptionBuilder error(final Throwable cause);

  /**
   * Builder for exceptions thrown by {@link #error}
   */
  interface ExceptionBuilder {
    /**
     * sets or replaces the error message.
     *
     * @see String#format(String, Object...)
     *
     * @param format format string
     * @param args Arguments referenced by the format specifiers in the format string
     * @return this builder
     */
    ExceptionBuilder message(final String format, final Object... args);

    /**
     * add a string line to the bottom of the context
     * @param value string line
     * @return this builder
     */
    ExceptionBuilder addContext(final String value);

    /**
     * add a string line to the bottom of the context
     * @param value string line
     * @return this builder
     */
    ExceptionBuilder addContext(final String value, Object... args);

    /**
     * add a string value to the bottom of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    ExceptionBuilder addContext(final String name, final String value);

    /**
     * Builds an exception that can be thrown by the caller
     */
    RuntimeException build();
  }
}
