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
package com.dremio.service.jobs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.AccessControlException;
import java.util.Optional;

import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.service.grpc.GrpcChannelBuilderFactory;
import com.dremio.service.grpc.GrpcServerBuilderFactory;
import com.dremio.type.AnyTypeUtil;
import com.google.common.base.Preconditions;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;

import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.ServerBuilder;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import io.grpc.stub.StreamObserver;

/**
 * Utilities related to RPC.
 */
public final class JobsRpcUtils {
  private static final Logger logger = LoggerFactory.getLogger(JobsRpcUtils.class);

  private static final String GRPC_STATUS_METADATA = "grpc-status-details-bin";
  private static final String IN_PROCESS_SERVICE_NAME = "local_jobs_service";
  private static final String JOBS_HOSTNAME = System.getProperty("services.jobs.hostname");
  private static final int JOBS_PORT = Integer.getInteger("services.jobs.port", 21467);
  private static final Metadata.BinaryMarshaller<Status> marshaller =
    ProtoLiteUtils.metadataMarshaller(Status.getDefaultInstance());
  /**
   * If RPC is over socket. Otherwise, use in process RPCs.
   *
   * Default is false.
   *
   * @return true iff RPC should be over socket
   */
  public static boolean isOverSocket() {
    return Boolean.getBoolean("dremio.service.jobs.over_socket");
  }

  /**
   * Create a new server builder.
   *
   * @return server builder
   */
  static ServerBuilder<?> newServerBuilder(GrpcServerBuilderFactory grpcFactory, int port) {
    if (!isOverSocket()) {
      return grpcFactory.newInProcessServerBuilder(IN_PROCESS_SERVICE_NAME);
    }

    return grpcFactory.newServerBuilder(port);
  }

  static String getJobsHostname() {
    return JOBS_HOSTNAME;
  }

  static int getJobsPort() {
    return JOBS_PORT;
  }

  /**
   * Create a new channel builder.
   *
   * @return channel builder
   */
  static ManagedChannelBuilder<?> newLocalChannelBuilder(GrpcChannelBuilderFactory grpcFactory, int port) {
    if (!isOverSocket()) {
      return grpcFactory.newInProcessChannelBuilder(IN_PROCESS_SERVICE_NAME);
    }
    try {
      return grpcFactory.newManagedChannelBuilder(InetAddress.getLocalHost().getCanonicalHostName(), port);
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Unable to get host name");
    }
  }

  /**
   * Converts the given {@link UserException} to a {@link StatusRuntimeException}.
   *
   * @param ue user exception
   * @return status runtime exception
   */
  static StatusRuntimeException toStatusRuntimeException(UserException ue) {
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
  static StatusRuntimeException toStatusRuntimeException(String message, Code code, UserException ue) {
    return StatusProto.toStatusRuntimeException(Status.newBuilder() // this is com.google.rpc.Status
        .setCode(code.value()) // this is io.grpc.Status.Code
        .setMessage(message)
        .addDetails(Any.pack(ue.getOrCreatePBError(false), AnyTypeUtil.DREMIO_TYPE_URL_PREFIX))
        .build());
  }

  private static Code toCode(ErrorType errorType) {
    switch (errorType) {
    case DATA_READ:
    case DATA_WRITE:
      return Code.FAILED_PRECONDITION;

    case PERMISSION:
      return Code.PERMISSION_DENIED;

    case RESOURCE:
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
   * Converts the given {@link FlightRuntimeException} to a {@link UserException}, if possible.
   *
   * @param fre status runtime exception
   * @return user exception if one is passed as part of the details
   */
  public static Optional<UserException> fromFlightRuntimeException(FlightRuntimeException fre) {
    Status statusProto = null;
    if (fre.status().metadata() != null) {
      ErrorFlightMetadata metadata = fre.status().metadata();
      if (metadata.containsKey(GRPC_STATUS_METADATA)) {
        statusProto = marshaller.parseBytes(metadata.getByte(GRPC_STATUS_METADATA));
      }
    }
    return (statusProto == null) ? Optional.empty() : fromStatus(statusProto);
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

  static <V> void handleException(StreamObserver<V> responseObserver, Throwable t) {
    Preconditions.checkNotNull(t, "exception");

    if (t instanceof UserException) {
      responseObserver.onError(toStatusRuntimeException((UserException) t));
    } else if (t instanceof JobNotFoundException) {
      responseObserver.onError(io.grpc.Status.NOT_FOUND.asException());
    } else if (t instanceof AccessControlException) {
      responseObserver.onError(io.grpc.Status.PERMISSION_DENIED.asRuntimeException());
    } else if (t instanceof StatusException) {
      responseObserver.onError(t);
    } else if (t instanceof StatusRuntimeException) {
      responseObserver.onError(t);
    } else if (t instanceof RuntimeException) {
      responseObserver.onError(io.grpc.Status.UNKNOWN.withDescription(t.getMessage())
          .asRuntimeException());
    } else {
      logger.warn("Unhandled exception", t);
      responseObserver.onError(io.grpc.Status.UNKNOWN.withDescription(t.getMessage())
          .asException());
    }
  }

  // prevent instantiation
  private JobsRpcUtils() {
  }
}
