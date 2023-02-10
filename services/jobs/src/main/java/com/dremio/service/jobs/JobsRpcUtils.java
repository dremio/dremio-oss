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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.GrpcExceptionUtil;
import com.dremio.common.exceptions.UserException;
import com.google.common.base.Preconditions;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Utilities related to RPC.
 */
public final class JobsRpcUtils {
  private static final Logger logger = LoggerFactory.getLogger(JobsRpcUtils.class);

  @SuppressWarnings("DremioGRPCStreamObserverOnError") //the exception is already a grpc exception
  static <V> void handleException(StreamObserver<V> responseObserver, Throwable t) {
    Preconditions.checkNotNull(t, "exception");
    responseObserver.onError(convertToGrpcException(t));
  }

  static Throwable convertToGrpcException(Throwable t) {
    Preconditions.checkNotNull(t, "exception");

    if (t instanceof UserException) {
      return GrpcExceptionUtil.toStatusRuntimeException((UserException) t);
    } else if (t instanceof JobNotFoundException) {
      if (((JobNotFoundException) t).getErrorType().equals(JobNotFoundException.causeOfFailure.CANCEL_FAILED)) {
        return Status.FAILED_PRECONDITION.withDescription(t.getMessage()).asRuntimeException();
      }
      return io.grpc.Status.NOT_FOUND.asException();
    } else if (t instanceof ReflectionJobValidationException) {
      return io.grpc.Status.INVALID_ARGUMENT.asException();
    } else if (t instanceof AccessControlException) {
      return io.grpc.Status.PERMISSION_DENIED.asRuntimeException();
    } else if (t instanceof StatusException) {
      return t;
    } else if (t instanceof StatusRuntimeException) {
      return t;
    } else if (t instanceof RuntimeException) {
      return io.grpc.Status.UNKNOWN.withDescription(t.getMessage()).asRuntimeException();
    } else {
      logger.warn("Unhandled exception", t);
      return io.grpc.Status.UNKNOWN.withDescription(t.getMessage()).asException();
    }
  }

  // prevent instantiation
  private JobsRpcUtils() {
  }
}
