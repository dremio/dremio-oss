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
package com.dremio.services.nessie.grpc.client;

import java.security.AccessControlException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;

import org.projectnessie.client.rest.NessieBadRequestException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieRefLogNotFoundException;
import org.projectnessie.error.NessieReferenceAlreadyExistsException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieReferenceNotFoundException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/** Maps gRPC exceptions to Nessie-specific exceptions and the other way around. */
public final class GrpcExceptionMapper {

  private GrpcExceptionMapper() {
  }

  /**
   * Takes the given Exception and converts it to a {@link StatusRuntimeException} with a specific
   * gRPC Status code.
   *
   * @param ex The exception to convert to a {@link StatusRuntimeException} with a specific gRPC
   *     Status code.
   * @return A new {@link StatusRuntimeException} with a specific gRPC status code.
   */
  public static StatusRuntimeException toProto(Exception ex) {
    if (ex instanceof BaseNessieClientServerException) {
      ErrorCode errorCode = ((BaseNessieClientServerException) ex).getErrorCode();
      Status status;
      if (ex instanceof NessieNotFoundException) {
        status = Status.NOT_FOUND;
      } else if (ex instanceof NessieConflictException) {
        status = Status.ALREADY_EXISTS;
      } else {
        status = Status.INVALID_ARGUMENT;
      }
      return status.withDescription((errorCode != null ? errorCode : ErrorCode.UNKNOWN).name())
        .augmentDescription(ex.getMessage())
        .withCause(ex)
        .asRuntimeException();
    }
    if (ex instanceof IllegalArgumentException) {
      return Status.INVALID_ARGUMENT
          .withDescription(ex.getMessage())
          .withCause(ex)
          .asRuntimeException();
    }
    if (ex instanceof ConstraintViolationException) {
      ConstraintViolationException cve = (ConstraintViolationException) ex;
      Set<ConstraintViolation<?>> violations = cve.getConstraintViolations();
      String message = violations.isEmpty() ? ex.getMessage() : violations.stream().map(v ->
        v.getPropertyPath() + ": " + v.getMessage()).collect(Collectors.joining(", "));
      return Status.INVALID_ARGUMENT
              .withDescription(message)
              .withCause(ex)
              .asRuntimeException();
    }
    if (ex instanceof AccessControlException) {
      return Status.PERMISSION_DENIED
          .withDescription(ex.getMessage())
          .withCause(ex)
          .asRuntimeException();
    }
    return Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException();
  }

  /**
   * Executes the given callable and reports the result to the given stream observer. Also performs
   * additional exception handling/conversion.
   *
   * @param callable The callable to call
   * @param <T> The type of the callable.
   * @param observer The {@link StreamObserver} where results are reported back to.
   */
  public static <T> void handle(Callable<T> callable, StreamObserver<T> observer) {
    try {
      observer.onNext(callable.call());
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  /**
   * Executes the given callable and performs additional exception handling/conversion.
   *
   * @param callable The callable to call
   * @param <T> The type of the callable.
   * @return The result of the callable
   * @throws NessieNotFoundException If the callable threw a gRPC exception, where the status
   *     matches a {@link NessieNotFoundException}
   * @throws NessieConflictException If the callable threw a gRPC exception, where the status
   *     matches a {@link NessieConflictException}
   * @throws NessieBadRequestException If the callable threw a gRPC exception, where the status
   *     matches a {@link NessieBadRequestException}
   * @throws StatusRuntimeException If the underlying exception couldn't be converted to the
   *     mentioned Nessie-specific exceptions, then a {@link StatusRuntimeException} with {@link
   *     Status#UNKNOWN} is thrown.
   */
  public static <T> T handle(Callable<T> callable)
      throws NessieNotFoundException, NessieConflictException {
    try {
      return callable.call();
    } catch (Exception e) {
      if (e instanceof StatusRuntimeException) {
        StatusRuntimeException sre = (StatusRuntimeException) e;
        if (isNotFound(sre)) {
          throw GrpcExceptionMapper.toNessieNotFoundException(sre);
        } else if (isAlreadyExists(sre)) {
          throw GrpcExceptionMapper.toNessieConflictException(sre);
        } else if (isInvalidArgument(sre)) {
          throw GrpcExceptionMapper.toNessieBadRequestException(sre);
        }
        throw sre;
      }
      throw Status.UNKNOWN.withCause(e).asRuntimeException();
    }
  }

  /**
   * Executes the given callable and performs additional exception handling/conversion.
   *
   * @param callable The callable to call
   * @param <T> The type of the callable.
   * @return The result of the callable
   * @throws NessieNotFoundException If the callable threw a gRPC exception, where the status
   *     matches a {@link NessieNotFoundException}
   * @throws NessieBadRequestException If the callable threw a gRPC exception, where the status
   *     matches a {@link NessieBadRequestException}
   * @throws StatusRuntimeException If the underlying exception couldn't be converted to the
   *     mentioned Nessie-specific exceptions, then a {@link StatusRuntimeException} with {@link
   *     Status#UNKNOWN} is thrown.
   */
  public static <T> T handleNessieNotFoundEx(Callable<T> callable) throws NessieNotFoundException {
    try {
      return callable.call();
    } catch (Exception e) {
      if (e instanceof StatusRuntimeException) {
        StatusRuntimeException sre = (StatusRuntimeException) e;
        if (isNotFound(sre)) {
          throw GrpcExceptionMapper.toNessieNotFoundException(sre);
        } else if (isInvalidArgument(sre)) {
          throw GrpcExceptionMapper.toNessieBadRequestException(sre);
        }
        throw sre;
      }
      throw Status.UNKNOWN.withCause(e).asRuntimeException();
    }
  }

  private static boolean isInvalidArgument(StatusRuntimeException sre) {
    return Status.INVALID_ARGUMENT.getCode() == sre.getStatus().getCode();
  }

  private static boolean isNotFound(StatusRuntimeException sre) {
    return Status.NOT_FOUND.getCode() == sre.getStatus().getCode();
  }

  private static boolean isAlreadyExists(StatusRuntimeException sre) {
    return Status.ALREADY_EXISTS.getCode() == sre.getStatus().getCode();
  }

  private static BaseNessieClientServerException toNessieException(StatusRuntimeException e, ImmutableNessieError.Builder nessieError, Function<NessieError, BaseNessieClientServerException> fallback) {
    String msg = e.getStatus().getDescription();
    if (msg != null) {
      int i = msg.indexOf('\n');
      try {
        ErrorCode errorCode = ErrorCode.valueOf(i == -1 ? msg : msg.substring(0, i));
        nessieError.errorCode(errorCode)
          .reason(errorCode.name())
          .message(msg.substring(i + 1));
        switch (errorCode) {
          case REFERENCE_ALREADY_EXISTS:
            return new NessieReferenceAlreadyExistsException(nessieError.build());
          case REFERENCE_CONFLICT:
            return new NessieReferenceConflictException(nessieError.build());
          case CONTENT_NOT_FOUND:
            return new NessieContentNotFoundException(nessieError.build());
          case REFERENCE_NOT_FOUND:
            return new NessieReferenceNotFoundException(nessieError.build());
          case REFLOG_NOT_FOUND:
            return new NessieRefLogNotFoundException(nessieError.build());
          default:
            break; // fall through
        }
      } catch (Exception unknown) {
        nessieError.message(msg);
      }
    } else {
      nessieError.message(e.getMessage());
    }

    return fallback.apply(nessieError.build());
  }

  private static NessieNotFoundException toNessieNotFoundException(StatusRuntimeException e) {
    return (NessieNotFoundException) toNessieException(e, ImmutableNessieError.builder()
        .message("Not found")
        .status(404),
      NessieNotFoundException::new);
  }

  private static NessieConflictException toNessieConflictException(StatusRuntimeException e) {
    return (NessieConflictException) toNessieException(e, ImmutableNessieError.builder()
        .message("Conflict")
        .status(409),
      NessieConflictException::new);
  }

  private static NessieBadRequestException toNessieBadRequestException(StatusRuntimeException e) {
    String msg = e.getStatus().getDescription();
    if (msg == null) {
      msg = e.getMessage();
    }

    ImmutableNessieError.Builder nessieError = ImmutableNessieError.builder()
      .message(msg)
      .status(400)
      .reason("Bad Request");
    return new NessieBadRequestException(nessieError.build());
  }
}
