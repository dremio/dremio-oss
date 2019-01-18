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
package com.dremio.common.exceptions;

import java.util.List;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.util.SchemaChangeRuntimeException;
import org.slf4j.Logger;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.google.protobuf.ByteString;

/**
 * Base class for all user exception. The goal is to separate out common error conditions where we can give users
 * useful feedback.
 * <p>Throwing a user exception will guarantee it's message will be displayed to the user, along with any context
 * information added to the exception at various levels while being sent to the client.
 * <p>A specific class of user exceptions are system exception. They represent system level errors that don't display
 * any specific error message to the user apart from "A system error has occurred" along with information to retrieve
 * the details of the exception from the logs.
 * <p>Although system exception should only display a generic message to the user, for now they will display the root
 * error message, until all user errors are properly sent from the server side.
 * <p>Any thrown exception that is not wrapped inside a user exception will automatically be converted to a system
 * exception before being sent to the client.
 *
 * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType
 */
public class UserException extends RuntimeException {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserException.class);

  public static final String MEMORY_ERROR_MSG = "Query was cancelled because it exceeded the memory limits set by the administrator.";

  /**
   * Creates a new INVALID_DATASET_METADATA exception builder.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#INVALID_DATASET_METADATA
   * @return user exception builder
   */
  public static Builder invalidMetadataError() {
    return invalidMetadataError(null);
  }

  /**
   * Wraps the passed exception inside a invalid dataset metadata error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * Add context with paths of data sets that should be refreshed. For example:
   * UserException.metadataOutOfDateError()
   *   .setAdditionalExceptionContext(new InvalidMetaDataErrorContext(paths))
   *   ...
   *   .build(logger);
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#INVALID_DATASET_METADATA
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder invalidMetadataError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.INVALID_DATASET_METADATA, cause);
  }

  /**
   * Creates a new SCHEMA_CHANGE exception builder.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#SCHEMA_CHANGE
   * @return user exception builder
   */
  public static Builder schemaChangeError() {
    return schemaChangeError(new SchemaChangeRuntimeException("Schema change error"));
  }

  /**
   * Wraps the passed exception inside a schema change error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#SCHEMA_CHANGE
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder schemaChangeError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.SCHEMA_CHANGE, cause);
  }

  /**
   * Creates an OUT_OF_MEMORY error with a prebuilt message
   *
   * @param cause exception that will be wrapped inside a memory error
   * @return user exception builder
   */
  public static Builder memoryError(final Throwable cause) {
    final Builder b = builder(DremioPBError.ErrorType.OUT_OF_MEMORY, cause)
      .message(MEMORY_ERROR_MSG);
    if (cause != null) {
      b.addContext(cause.getMessage());
    }
    return b;
  }

  /**
   * Creates an OUT_OF_MEMORY error with a prebuilt message
   *
   * @return user exception builder
   */
  public static Builder memoryError() {
    return memoryError(null);
  }

  /**
   * Wraps the passed exception inside a system error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#SYSTEM
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   *
   * @deprecated This method should never need to be used explicitly, unless you are passing the exception to the
   *             Rpc layer or UserResultListener.submitFailed()
   */
  @Deprecated
  public static Builder systemError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.SYSTEM, cause);
  }

  /**
   * Creates a new user exception builder.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#CONNECTION
   * @return user exception builder
   */
  public static Builder connectionError() {
    return connectionError(null);
  }

  /**
   * Wraps the passed exception inside a connection error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#CONNECTION
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder connectionError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.CONNECTION, cause);
  }

  /**
   * Creates a new user exception builder.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#DATA_READ
   * @return user exception builder
   */
  public static Builder dataReadError() {
    return dataReadError(null);
  }

  /**
   * Wraps the passed exception inside a data read error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#DATA_READ
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder dataReadError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.DATA_READ, cause);
  }

  /**
   * Creates a new user exception builder.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#SOURCE_BAD_STATE
   * @return user exception builder
   */
  public static Builder sourceInBadState() {
    return sourceInBadState(null);
  }

  /**
   * Wraps the passed exception inside a data read error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#SOURCE_BAD_STATE
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder sourceInBadState(final Throwable cause) {
    return builder(DremioPBError.ErrorType.SOURCE_BAD_STATE, cause);
  }

  /**
   * Creates a new user exception builder .
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#DATA_WRITE
   * @return user exception builder
   */
  public static Builder dataWriteError() {
    return dataWriteError(null);
  }

  /**
   * Wraps the passed exception inside a data write error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#DATA_WRITE
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder dataWriteError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.DATA_WRITE, cause);
  }

  /**
   * Creates a new user exception builder .
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#FUNCTION
   * @return user exception builder
   */
  public static Builder functionError() {
    return functionError(null);
  }

  /**
   * Wraps the passed exception inside a function error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#FUNCTION
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder functionError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.FUNCTION, cause);
  }

  /**
   * Creates a new user exception builder .
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#PARSE
   * @return user exception builder
   */
  public static Builder parseError() {
    return parseError(null);
  }

  /**
   * Wraps the passed exception inside a parse error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#PARSE
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder parseError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.PARSE, cause);
  }

  /**
   * Creates a new user exception builder .
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#VALIDATION
   * @return user exception builder
   */
  public static Builder validationError() {
    return validationError(null);
  }

  /**
   * wraps the passed exception inside a validation error.
   * <p>the cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>if the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#VALIDATION
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder validationError(Throwable cause) {
    return builder(DremioPBError.ErrorType.VALIDATION, cause);
  }

  /**
   * Creates a new user exception builder .
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#REFLECTION_ERROR
   * @return user exception builder
   */
  public static Builder reflectionError() {
    return reflectionError(null);
  }

  /**
   * wraps the passed exception inside a validation error.
   * <p>the cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>if the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#REFLECTION_ERROR
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder reflectionError(Throwable cause) {
    return builder(DremioPBError.ErrorType.REFLECTION_ERROR, cause);
  }

  /**
   * creates a new user exception builder .
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#PERMISSION
   * @return user exception builder
   */
  public static Builder permissionError() {
    return permissionError(null);
  }

  /**
   * Wraps the passed exception inside a permission error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#PERMISSION
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder permissionError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.PERMISSION, cause);
  }

  /**
   * Creates a new user exception builder .
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#PLAN
   * @return user exception builder
   */
  public static Builder planError() {
    return planError(null);
  }

  /**
   * Wraps the passed exception inside a plan error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#PLAN
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder planError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.PLAN, cause);
  }

  /**
   * Creates a new user exception builder .
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#RESOURCE
   * @return user exception builder
   */
  public static Builder resourceError() {
    return resourceError(null);
  }

  /**
   * Wraps the passed exception inside a resource error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#RESOURCE
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder resourceError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.RESOURCE, cause);
  }

  /**
   * Creates a new user exception builder .
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#UNSUPPORTED_OPERATION
   * @return user exception builder
   */
  public static Builder unsupportedError() {
    return unsupportedError(null);
  }

  /**
   * Wraps the passed exception inside a unsupported error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#UNSUPPORTED_OPERATION
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder unsupportedError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.UNSUPPORTED_OPERATION, cause);
  }

  /**
   * Creates a new user exception builder .
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#IO_EXCEPTION
   * @return user exception builder
   */
  public static Builder ioExceptionError() {
    return ioExceptionError(null);
  }

  /**
   * Wraps the passed exception inside a ioexception error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#IO_EXCEPTION
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder ioExceptionError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.IO_EXCEPTION, cause);
  }

  /**
   * Creates a new user exception builder .
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#CONCURRENT_MODIFICATION
   * @return user exception builder
   */
  public static Builder concurrentModificationError() {
    return concurrentModificationError(null);
  }

  /**
   * Wraps the passed exception inside a concurrent modification error.
   * <p>The cause message will be used unless {@link Builder#message(String, Object...)} is called.
   * <p>If the wrapped exception is, or wraps, a user exception it will be returned by {@link Builder#build(Logger)}
   * instead of creating a new exception. Any added context will be added to the user exception as well.
   *
   * @see com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType#CONCURRENT_MODIFICATION
   *
   * @param cause exception we want the user exception to wrap. If cause is, or wrap, a user exception it will be
   *              returned by the builder instead of creating a new user exception
   * @return user exception builder
   */
  public static Builder concurrentModificationError(final Throwable cause) {
    return builder(DremioPBError.ErrorType.CONCURRENT_MODIFICATION, cause);
  }

  protected static Builder builder(final DremioPBError.ErrorType type, final Throwable cause) {
    return new Builder(type, cause);
  }

  /**
   * Builder class for UserException. You can wrap an existing exception, in this case it will first check if
   * this exception is, or wraps, a UserException. If it does then the builder will use the user exception as it is
   * (it will ignore the message passed to the constructor) and will add any additional context information to the
   * exception's context
   */
  public static class Builder {

    private final Throwable cause;
    private final DremioPBError.ErrorType errorType;
    private final UserException uex;
    private final UserExceptionContext context;

    private String message;
    private ByteString rawAdditionalContext;

    private boolean fixedMessage; // if true, calls to message() are a no op

    /**
     * Wraps an existing exception inside a user exception.
     *
     * traverses the causes hierarchy searching for out of memory exception, if found the exception will be converted
     * to a memory error
     *
     * @param errorType user exception type that should be created if the passed exception isn't,
     *                  or doesn't wrap a user exception
     * @param cause exception to wrap inside a user exception. Can be null
     */
    private Builder(final DremioPBError.ErrorType errorType, final Throwable cause) {

      //TODO handle the improbable case where cause is a SYSTEM exception ?
      uex = ErrorHelper.findWrappedCause(cause, UserException.class);
      if (uex != null) {
        this.errorType = null;
        this.context = uex.context;
        this.cause = cause;
      } else {
        OutOfMemoryException oom = ErrorHelper.findWrappedCause(cause, OutOfMemoryException.class);
        if (oom != null) {
          this.errorType = DremioPBError.ErrorType.OUT_OF_MEMORY;
          this.message = MEMORY_ERROR_MSG;
          this.cause = oom;
          fixedMessage = true;
        } else {
          // we will create a new user exception
          this.cause = cause;
          this.errorType = errorType;
          this.message = cause != null ? cause.getMessage() : null;
        }
        this.context = new UserExceptionContext();
      }
    }

    /**
     * sets or replaces the error message.
     * <p>This will be ignored if this builder is wrapping a user exception
     *
     * @see String#format(String, Object...)
     *
     * @param format format string
     * @param args Arguments referenced by the format specifiers in the format string
     * @return this builder
     */
    public Builder message(final String format, final Object... args) {
      // we can't replace the message of a user exception
      if (uex == null && !fixedMessage && format != null) {
        this.message = (args == null || args.length == 0) ? format : String.format(format, args);
      }
      return this;
    }

    /**
     * add NodeEndpoint identity to the context.
     * <p>if the context already has a node endpoint identity, the new identity will be ignored
     *
     * @param endpoint node endpoint identity
     */
    public Builder addIdentity(final CoordinationProtos.NodeEndpoint endpoint) {
      context.add(endpoint);
      return this;
    }

    /**
     * add a string line to the bottom of the context
     * @param value string line
     * @return this builder
     */
    public Builder addContext(final String value) {
      context.add(value);
      return this;
    }

    /**
     * add a string line to the bottom of the context
     * @param value string line
     * @return this builder
     */
    public Builder addContext(final String value, Object... args) {
      context.add(String.format(value, args));
      return this;
    }

    /**
     * add a string value to the bottom of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    public Builder addContext(final String name, final String value) {
      context.add(name, value);
      return this;
    }

    /**
     * add a long value to the bottom of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    public Builder addContext(final String name, final long value) {
      context.add(name, value);
      return this;
    }

    /**
     * add a double value to the bottom of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    public Builder addContext(final String name, final double value) {
      context.add(name, value);
      return this;
    }

    /**
     * pushes a string value to the top of the context
     *
     * @param value context value
     * @return this builder
     */
    public Builder pushContext(final String value) {
      context.push(value);
      return this;
    }

    /**
     * pushes a string value to the top of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    public Builder pushContext(final String name, final String value) {
      context.push(name, value);
      return this;
    }

    /**
     * pushes a long value to the top of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    public Builder pushContext(final String name, final long value) {
      context.push(name, value);
      return this;
    }

    /**
     * pushes a double value to the top of the context
     *
     * @param name context name
     * @param value context value
     * @return this builder
     */
    public Builder pushContext(final String name, final double value) {
      context.push(name, value);
      return this;
    }

    /**
     * Set type specific additional contextual information.
     *
     * @param additionalContext additional additional context
     * @return this builder
     */
    public Builder setAdditionalExceptionContext(AdditionalExceptionContext additionalContext) {
      assert additionalContext.getErrorType() == errorType : "error type of context and builder must match";
      this.rawAdditionalContext = additionalContext.toByteString();
      return this;
    }

    /**
     * builds a user exception or returns the wrapped one. If the error is a system error, the error message is logged
     * to the given {@link Logger}.
     *
     * @param logger the logger to write to
     * @return user exception
     */
    public UserException build(final Logger logger) {
      if (uex != null) {
        return uex;
      }

      boolean isSystemError = errorType == DremioPBError.ErrorType.SYSTEM;

      // make sure system errors use the root error message and display the root cause class name
      if (isSystemError) {
        message = ErrorHelper.getRootMessage(cause);
      } else if (message == null || message.length() == 0) {
        // make sure message is not null or empty, otherwise the user will be completely clueless
        // and it may cause NPE when we generate the corresponding DremioPBError object
        if (cause != null) {
          // this case is possible if a user exception wraps a cause with null message (NPE ?)
          // and we don't set a custom error message
          message = cause.getClass().getSimpleName();
        } else {
          // we should never hit this unless we are using user exception in the wrong way
          message = "";
        }
      }

      final UserException newException = new UserException(this);

      // since we just created a new exception, we should log it for later reference. If this is a system error, this is
      // an issue that the system admin should pay attention to and we should log as ERROR. However, if this is a user
      // mistake or data read issue, the system admin should not be concerned about these and thus we'll log this
      // as an INFO message.
      switch(errorType) {
      case SYSTEM:
        logger.error(newException.getMessage(), newException);
        break;
      case OUT_OF_MEMORY:
        logger.error(newException.getMessage(), newException);
        break;
      case IO_EXCEPTION:
        logger.debug(newException.getMessage(), newException);
        break;
      default:
        logger.info("User Error Occurred [" + newException.getErrorIdWithIdentity() + "]", newException);
      }

      return newException;
    }

    /**
     * Builds a user exception or returns the wrapped one.
     *
     * @return user exception
     * @deprecated Use {@link #build(Logger)} instead. If the error is a system error, the error message is logged to
     * this {@link UserException#logger}.
     */
    @Deprecated
    public UserException build() {
      return build(logger);
    }
  }

  private final DremioPBError.ErrorType errorType;

  private final UserExceptionContext context;

  private final ByteString rawAdditionalContext;

  protected UserException(final DremioPBError.ErrorType errorType, final String message, final Throwable cause,
                          final ByteString rawAdditionalContext) {
    super(message, cause);

    this.errorType = errorType;
    this.context = new UserExceptionContext();
    this.rawAdditionalContext = rawAdditionalContext;
  }

  private UserException(final Builder builder) {
    super(builder.message, builder.cause);
    this.errorType = builder.errorType;
    this.context = builder.context;
    this.rawAdditionalContext = builder.rawAdditionalContext;
  }

  /**
   * generates the message that will be displayed to the client without the stack trace.
   *
   * @return non verbose error message
   */
  @Override
  public String getMessage() {
    return super.getMessage();
  }

  public String getErrorIdWithIdentity() {
    final NodeEndpoint endpoint = context.getEndpoint();

    if (endpoint == null ) {
      return "ErrorId: " + context.getErrorId();
    } else {
      return "ErrorId: " + context.getErrorId() + " on " + endpoint.getAddress() + ":" + endpoint.getUserPort();
    }
  }

  /**
   *
   * @return the error message that was passed to the builder
   */
  public String getOriginalMessage() {
    return super.getMessage();
  }

  /**
   * generates the message that will be displayed to the client. The message also contains the stack trace.
   *
   * @return verbose error message
   */
  public String getVerboseMessage() {
    return getVerboseMessage(true);
  }

  public String getVerboseMessage(boolean includeErrorIdAndIdentity) {
    return generateMessage(includeErrorIdAndIdentity) + "\n\n" + ErrorHelper.buildCausesMessage(getCause());
  }

  /**
   * returns or creates a DremioPBError object corresponding to this user exception.
   *
   * @param verbose should the error object contain the verbose error message ?
   * @return protobuf error object
   */
  public DremioPBError getOrCreatePBError(final boolean verbose) {
    final DremioPBError.Builder builder = DremioPBError.newBuilder();
    builder.setErrorType(errorType);
    builder.setErrorId(context.getErrorId());
    if (context.getEndpoint() != null) {
      builder.setEndpoint(context.getEndpoint());
    }
    builder.setMessage(getVerboseMessage());
    builder.setOriginalMessage(getOriginalMessage());

    if (getCause() != null) {
      // some unit tests use this information to make sure a specific exception was thrown in the server
      builder.setException(ErrorHelper.getWrapper(getCause()));
    }

    builder.addAllContext(context.getContextAsStrings());

    if (rawAdditionalContext != null) {
        builder.setTypeSpecificContext(rawAdditionalContext);
    }
    return builder.build();
  }

  /**
   * Get serialized type specific additional contextual information.
   *
   * @return additional context in ByteString format.
   */
  public ByteString getRawAdditionalExceptionContext() {
    return rawAdditionalContext;
  }

  public List<String> getContextStrings() {
    return context.getContextAsStrings();
  }

  public String getErrorId() {
    return context.getErrorId();
  }

  public DremioPBError.ErrorType getErrorType() {
    return errorType;
  }

  public String getErrorLocation() {
    NodeEndpoint ep = context.getEndpoint();
    if (ep != null) {
      return ep.getAddress() + ":" + ep.getUserPort();
    } else {
      return null;
    }
  }

  /**
   * Generates a user error message that has the following structure:
   * ERROR TYPE: ERROR_MESSAGE
   * CONTEXT
   * [ERROR_ID on NODE_IP:NODE_USER_PORT]
   *
   * @return generated user error message
   */
  private String generateMessage(boolean includeErrorIdAndIdentity) {
    return errorType + " ERROR: " + super.getMessage() + "\n\n" +
        context.generateContextMessage(includeErrorIdAndIdentity);
  }
}
