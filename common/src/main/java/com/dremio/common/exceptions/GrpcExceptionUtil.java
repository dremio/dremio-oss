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
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

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
