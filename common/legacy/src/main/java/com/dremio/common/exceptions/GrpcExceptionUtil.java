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

package com.dremio.common.exceptions;

import java.security.AccessControlException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.type.AnyTypeUtil;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;

/**
 * Utility functions related to grpc errors.
 */
public final class GrpcExceptionUtil {
  private static final Logger logger = LoggerFactory.getLogger(GrpcExceptionUtil.class);

  private static Code toCode(ErrorType errorType) {
    switch (errorType) {
    case DATA_READ:
    case DATA_WRITE:
      return Code.FAILED_PRECONDITION;

    case PERMISSION:
      return Code.PERMISSION_DENIED;

    case RESOURCE:
    case RESOURCE_TIMEOUT:
      return Code.RESOURCE_EXHAUSTED;

    case UNSUPPORTED_OPERATION:
      return Code.UNIMPLEMENTED;

    case PARSE:
    case PLAN:
    case VALIDATION:
      return Code.INVALID_ARGUMENT;

    case CONCURRENT_MODIFICATION:
      return Code.ABORTED;

    case FUNCTION:
    case IO_EXCEPTION:
    case CONNECTION:
    case SCHEMA_CHANGE:
    case INVALID_DATASET_METADATA:
    case REFLECTION_ERROR:
    case SOURCE_BAD_STATE:
    case JSON_FIELD_CHANGE:
    case SYSTEM:
    case OUT_OF_MEMORY:
      return Code.INTERNAL;

    default:
      return Code.UNKNOWN;
    }
  }

  public static StatusRuntimeException toStatusRuntimeException(DremioPBError error) {
    return toStatusRuntimeException(error.getMessage(), toCode(error.getErrorType()), UserRemoteException.create(error));
  }

  /**
   * Converts the given {@link UserException} to a {@link StatusRuntimeException}.
   *
   * @param ue user exception
   * @return status runtime exception
   */
  public static StatusRuntimeException toStatusRuntimeException(UserException ue) {
    return toStatusRuntimeException(ue.getMessage(), toCode(ue.getErrorType()), ue);
  }

  /**
   * Converts the given {@link UserException} to a {@link StatusRuntimeException}.
   *
   * @param message status message
   * @param code    status code
   * @param ue      user exception
   * @return status runtime exception
   */
  public static StatusRuntimeException toStatusRuntimeException(String message, Code code, UserException ue) {
    return StatusProto.toStatusRuntimeException(Status.newBuilder() // this is com.google.rpc.Status
        .setCode(code.value()) // this is io.grpc.Status.Code
        .setMessage(message)
        .addDetails(Any.pack(ue.getOrCreatePBError(false), AnyTypeUtil.DREMIO_TYPE_URL_PREFIX))
        .build());
  }

  /**
   *  Handles unknown {@link Throwable} by passing it in the {@link StreamObserver} as a Status* exception
   *  Only use this method after handling the throwable as accurately as possible, and when no other information about the throwable is available
   * @param responseObserver responseObserver
   * @param t unknown exception
   * @param message High level description of what failed (can be found from the method name)
   */
  public static <V> void fallbackHandleException(StreamObserver<V> responseObserver, Throwable t, String message) {
    logger.warn("Using fallback to handle unknown exception", t);
    if (t instanceof UserException) {
      responseObserver.onError(toStatusRuntimeException((UserException) t));
    } else if (t instanceof StatusException) {
      responseObserver.onError((StatusException) t);
    } else if (t instanceof StatusRuntimeException) {
      responseObserver.onError(statusRuntimeExceptionMapper(t));
    } else {
      responseObserver.onError(toErrorStatus(t, message).asRuntimeException());
    }
  }

  /**
   * Returns Grpc status given an exception type
   */
  public static io.grpc.Status toErrorStatus(Throwable t, String message) {
    if (t instanceof IllegalArgumentException) {
      return io.grpc.Status.INVALID_ARGUMENT
          .withCause(t)
          .withDescription(message);
    } else if (t instanceof IllegalStateException) {
      return io.grpc.Status.INTERNAL
          .withCause(t)
          .withDescription(message);
    } else if (t instanceof AccessControlException) {
      return io.grpc.Status.PERMISSION_DENIED
          .withCause(t)
          .withDescription(message);
    } else {
      return io.grpc.Status.UNKNOWN
          .withCause(t)
          .withDescription(message);
    }
  }

  private static StatusRuntimeException statusRuntimeExceptionMapper(Throwable t) {
    if (!(t instanceof StatusRuntimeException)) {
      return new StatusRuntimeException(
        io.grpc.Status.UNKNOWN.withDescription(
          "The server encountered an unexpected error. Please retry your request.")
          .withCause(t));
    }

    StatusRuntimeException sre = (StatusRuntimeException) t;
    // UNAVAILABLE error is shown as "UNAVAILABLE: no healthy upstream" to the user.
    // Provide a readable error message to user.
    if (sre.getStatus().getCode() == io.grpc.Status.Code.UNAVAILABLE) {
      return new StatusRuntimeException(
        io.grpc.Status.UNAVAILABLE.withDescription(
          "The service is temporarily unavailable. Please retry your request.")
          .withCause(t));
    }
    return sre;
  }

  /**
   *  Handles unknown {@link Throwable} by passing it in the {@link StreamObserver} as a Status* exception
   *  Only use this method after handling the throwable as accurately as possible, and when no other information about the throwable is available
   * @param responseObserver responseObserver
   * @param t unknown exception
   */
  public static <V> void fallbackHandleException(StreamObserver<V> responseObserver, Throwable t) {
    fallbackHandleException(responseObserver, t, t.getMessage());
  }

  /**
   * Converts the given {@link StatusRuntimeException} to a {@link UserException}, if possible.
   *
   * @param sre status runtime exception
   * @return user exception if one is passed as part of the details
   */
  public static Optional<UserException> fromStatusRuntimeException(StatusRuntimeException sre) {
    final Status statusProto = StatusProto.fromThrowable(sre);
    if (statusProto == null) {
      return Optional.empty();
    }
    return fromStatus(statusProto);
  }

  /**
   * Converts the given {@link Status} to a {@link UserException}, if possible.
   *
   * @param statusProto status runtime exception
   * @return user exception if one is passed as part of the details
   */
  public static Optional<UserException> fromStatus(Status statusProto) {
    for (Any details : statusProto.getDetailsList()) {
      if (details.is(DremioPBError.class)) {
        try {
          return Optional.of(UserRemoteException.create(details.unpack(DremioPBError.class)));
        } catch (InvalidProtocolBufferException e) {
          logger.warn("Received an invalid UserException, ignoring", e);
          return Optional.empty();
        }
      }
    }

    return Optional.empty();
  }

  /**
   * Throws if the given exception contains a {@link UserException} as part of its details.
   * @param e exception
   */
  public static void throwIfUserException(StatusRuntimeException e) {
    final Optional<UserException> userException = fromStatusRuntimeException(e);
    if (userException.isPresent()) {
      throw userException.get();
    }
  }

  private GrpcExceptionUtil() {
  }
}
