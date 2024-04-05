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
package com.dremio.services.nessie.grpc;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_INVALID_SUBTYPE;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ErrorCodeAware;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieErrorDetails;
import org.projectnessie.error.NessieForbiddenException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceAlreadyExistsException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieReferenceNotFoundException;

/** Maps gRPC exceptions to Nessie-specific exceptions and the other way around. */
public final class GrpcExceptionMapper {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(GrpcExceptionMapper.class);

  /**
   * Object mapper that ignores unknown properties and unknown subtypes, so it is able to process
   * instances of {@link NessieError} and especially {@link
   * org.projectnessie.error.NessieErrorDetails} with added/unknown properties or unknown subtypes
   * of the latter.
   */
  private static final ObjectMapper MAPPER =
      new ObjectMapper().disable(FAIL_ON_UNKNOWN_PROPERTIES).disable(FAIL_ON_INVALID_SUBTYPE);

  private static final Metadata.Key<byte[]> NESSIE_ERROR_KEY =
      Metadata.Key.of("nessie-error-bin", Metadata.BINARY_BYTE_MARSHALLER);

  private static final ServerExceptionMapper SERVER_EXCEPTION_MAPPER = loadServerMapper();

  private GrpcExceptionMapper() {}

  /**
   * Takes the given Exception and converts it to a {@link StatusRuntimeException} with a specific
   * gRPC Status code.
   *
   * @param ex The exception to convert to a {@link StatusRuntimeException} with a specific gRPC
   *     Status code.
   * @return A new {@link StatusRuntimeException} with a specific gRPC status code.
   */
  public static StatusRuntimeException toProto(Exception ex) {
    return SERVER_EXCEPTION_MAPPER.toProto(ex);
  }

  private static StatusRuntimeException toProtoDefault(Exception ex) {
    if (ex instanceof ErrorCodeAware) {
      return statusRuntimeExFromNessieEx((ErrorCodeAware) ex);
    }
    if (ex.getCause() instanceof ErrorCodeAware) {
      return statusRuntimeExFromNessieEx((ErrorCodeAware) ex.getCause());
    }
    if (ex instanceof IllegalArgumentException) {
      return statusRuntimeExForBadRequest(ex.getMessage(), ex);
    }
    logger.warn("Handling internal exception", ex);
    return Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException();
  }

  private static Metadata toProto(NessieError nessieError) {
    Metadata metadata = new Metadata();
    try {
      byte[] bytes = MAPPER.writerFor(NessieError.class).writeValueAsBytes(nessieError);
      metadata.put(NESSIE_ERROR_KEY, bytes);
    } catch (JsonProcessingException e) {
      logger.error("Unable to serialize NessieError.", e);
    }
    return metadata;
  }

  private static NessieError fromProto(Metadata metadata) {
    if (metadata == null) {
      return null;
    }

    byte[] bytes = metadata.get(NESSIE_ERROR_KEY);
    if (bytes == null) {
      return null;
    }

    try {
      return MAPPER.readValue(bytes, NessieError.class);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to deserialize NessieError: " + e, e);
    }
  }

  public static StatusRuntimeException statusRuntimeExForBadRequest(
      String message, Exception cause) {
    Status status = Status.INVALID_ARGUMENT;

    Metadata trailers =
        toProto(
            ImmutableNessieError.builder()
                .status(ErrorCode.BAD_REQUEST.httpStatus())
                .reason("Bad Request")
                .message(message)
                .errorCode(ErrorCode.BAD_REQUEST)
                .build());

    return status.withDescription(message).withCause(cause).asRuntimeException(trailers);
  }

  public static StatusRuntimeException statusRuntimeExFromNessieEx(ErrorCodeAware error) {
    Throwable ex = (Throwable) error;
    ErrorCode errorCode = error.getErrorCode();
    NessieErrorDetails errorDetails = error.getErrorDetails();

    Status status;
    if (ex instanceof NessieNotFoundException) {
      status = Status.NOT_FOUND;
    } else if (ex instanceof NessieConflictException) {
      status = Status.ALREADY_EXISTS;
    } else if (ex instanceof NessieForbiddenException) {
      status = Status.PERMISSION_DENIED;
    } else {
      status = Status.INVALID_ARGUMENT;
    }

    Metadata trailers =
        toProto(
            ImmutableNessieError.builder()
                .status(errorCode.httpStatus())
                .reason(errorCode.name())
                .message(ex.getMessage())
                .errorCode(errorCode)
                .errorDetails(errorDetails)
                .build());

    return status
        .withDescription(errorCode.name())
        .augmentDescription(
            ex.getMessage()) // keep old exception data for compatibility with old clients
        .withCause(ex)
        .asRuntimeException(trailers);
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
        throwDeclaredException(sre, NessieNotFoundException.class, NessieConflictException.class);
        throw sre; // unreachable
      }
      logger.warn("Handling unknown exception", e);
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
        throwDeclaredException(sre, NessieNotFoundException.class, RuntimeException.class);
        throw sre; // unreachable
      }
      logger.warn("Handling unknown exception", e);
      throw Status.UNKNOWN.withCause(e).asRuntimeException();
    }
  }

  /**
   * Executes the given callable and performs additional exception handling/conversion.
   *
   * @param callable The callable to call
   * @param <T> The type of the callable.
   * @return The result of the callable
   * @throws NessieBadRequestException If the callable threw a gRPC exception, where the status
   *     matches a {@link NessieBadRequestException}
   * @throws StatusRuntimeException If the underlying exception couldn't be converted to the
   *     mentioned Nessie-specific exceptions, then a {@link StatusRuntimeException} with {@link
   *     Status#UNKNOWN} is thrown.
   */
  public static <T> T handleNessieRuntimeEx(Callable<T> callable) {
    try {
      return callable.call();
    } catch (Exception e) {
      if (e instanceof StatusRuntimeException) {
        StatusRuntimeException sre = (StatusRuntimeException) e;
        throwDeclaredException(sre, RuntimeException.class, RuntimeException.class);
        throw sre;
      }
      logger.warn("Handling unknown exception", e);
      throw Status.UNKNOWN.withCause(e).asRuntimeException();
    }
  }

  private static <E1 extends Exception, E2 extends Exception> void throwDeclaredException(
      StatusRuntimeException e, Class<E1> scope1, Class<E2> scope2) throws E1, E2 {

    Exception ex = toNessieException(e);

    if (scope1.isInstance(ex)) {
      throw scope1.cast(ex);
    }

    if (scope2.isInstance(ex)) {
      throw scope2.cast(ex);
    }

    if (ex instanceof RuntimeException) {
      throw (RuntimeException) ex;
    }

    throw new UndeclaredThrowableException(
        ex,
        "Undeclared exception: "
            + ex.getClass().getName()
            + ":"
            + ex
            + " (allowed types: "
            + scope1.getName()
            + ", "
            + scope2.getName()
            + ")");
  }

  private static Exception toNessieException(StatusRuntimeException e) {
    NessieError error = fromProto(e.getTrailers());
    if (error != null) {
      Optional<Exception> modelException = ErrorCode.asException(error);
      if (modelException.isPresent()) {
        return modelException.get();
      }
    }

    // Fallback for older clients
    ImmutableNessieError.Builder nessieError = ImmutableNessieError.builder();
    String msg = e.getStatus().getDescription();
    if (msg != null) {
      int i = msg.indexOf('\n');
      try {
        ErrorCode errorCode;
        if (i < 0) {
          errorCode = ErrorCode.UNKNOWN;
        } else {
          errorCode = ErrorCode.valueOf(msg.substring(0, i));
        }

        String message = msg.substring(i + 1);
        nessieError.errorCode(errorCode).reason(errorCode.name()).message(message);

        switch (e.getStatus().getCode()) {
          case NOT_FOUND:
            nessieError.status(404);
            break;
          case ALREADY_EXISTS:
            nessieError.status(409);
            break;
          case INVALID_ARGUMENT:
            nessieError.status(400);
            break;
          default:
            nessieError.status(500);
            break;
        }

        switch (errorCode) {
          case REFERENCE_ALREADY_EXISTS:
            return new NessieReferenceAlreadyExistsException(nessieError.build());
          case REFERENCE_CONFLICT:
            return new NessieReferenceConflictException(nessieError.build());
          case CONTENT_NOT_FOUND:
            return new NessieContentNotFoundException(nessieError.build());
          case REFERENCE_NOT_FOUND:
            return new NessieReferenceNotFoundException(nessieError.build());
          case BAD_REQUEST:
            return new NessieBadRequestException(nessieError.build());
          case NAMESPACE_ALREADY_EXISTS:
            return new NessieNamespaceAlreadyExistsException(nessieError.build());
          case NAMESPACE_NOT_EMPTY:
            return new NessieNamespaceNotEmptyException(nessieError.build());
          case NAMESPACE_NOT_FOUND:
            return new NessieNamespaceNotFoundException(nessieError.build());
          case UNKNOWN:
            // Generic exceptions without an error code. These exceptions are never thrown by modern
            // Nessie Servers,
            // but are handled here for the sake of completeness and compatibility with older
            // servers.
            switch (e.getStatus().getCode()) {
              case NOT_FOUND:
              case ALREADY_EXISTS:
              case INVALID_ARGUMENT:
                return new NessieBadRequestException(
                    nessieError
                        .errorCode(ErrorCode.BAD_REQUEST)
                        .reason("Bad Request: " + e.getStatus().getCode().name())
                        .build());
              default:
                break;
            }
            break;
          default:
            break; // fall through
        }
      } catch (Exception unknown) {
        nessieError.message(msg);
      }
    } else {
      nessieError.message(e.getMessage());
    }

    return e;
  }

  private static ServerExceptionMapper loadServerMapper() {
    ServiceLoader<ServerExceptionMapper> implementations =
        ServiceLoader.load(ServerExceptionMapper.class);

    ServerExceptionMapper mapper = GrpcExceptionMapper::toProtoDefault;
    for (ServerExceptionMapper impl : implementations) {
      logger.info("Using ServerExceptionMapper: {}", impl.getClass().getName());

      ServerExceptionMapper base = mapper;
      mapper =
          ex -> {
            StatusRuntimeException mapped = impl.toProto(ex);
            return mapped != null ? mapped : base.toProto(ex);
          };
    }
    return mapper;
  }
}
