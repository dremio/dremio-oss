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

import java.security.AccessControlException;
import java.util.Optional;

import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.GrpcExceptionUtil;
import com.dremio.common.exceptions.UserException;
import com.google.common.base.Preconditions;
import com.google.rpc.Status;

import io.grpc.Metadata;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import io.grpc.stub.StreamObserver;

/**
 * Utilities related to RPC.
 */
public final class JobsRpcUtils {
  private static final Logger logger = LoggerFactory.getLogger(JobsRpcUtils.class);

  private static final String GRPC_STATUS_METADATA = "grpc-status-details-bin";
  private static final Metadata.BinaryMarshaller<Status> marshaller =
    ProtoLiteUtils.metadataMarshaller(Status.getDefaultInstance());

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
    return (statusProto == null) ? Optional.empty() : GrpcExceptionUtil.fromStatus(statusProto);
  }

  static <V> void handleException(StreamObserver<V> responseObserver, Throwable t) {
    Preconditions.checkNotNull(t, "exception");

    if (t instanceof UserException) {
      responseObserver.onError(GrpcExceptionUtil.toStatusRuntimeException((UserException) t));
    } else if (t instanceof JobNotFoundException) {
      responseObserver.onError(io.grpc.Status.NOT_FOUND.asException());
    } else if (t instanceof ReflectionJobValidationException) {
      responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT.asException());
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
