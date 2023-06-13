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

import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handle;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handleNessieNotFoundEx;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.toProto;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.AccessControlException;
import java.util.Collections;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.ContentKeyErrorDetails;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.ImmutableReferenceConflicts;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceAlreadyExistsException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Tests for the {@link GrpcExceptionMapper}
 */
public class TestGrpcExceptionMapper {

  private static final ContentKey CONTENT_KEY = ContentKey.of("folder1", "folder2");
  private static final ContentKeyErrorDetails CONTENT_ERROR_DETAILS =
    ContentKeyErrorDetails.contentKeyErrorDetails(CONTENT_KEY);

  @Test
  public void exceptionToProtoConversion() {
    assertThat(toProto(new IllegalArgumentException("x")))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.INVALID_ARGUMENT.getCode());

    assertThat(toProto(new NessieReferenceNotFoundException("not found")))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.NOT_FOUND.getCode());

    assertThat(toProto(new NessieReferenceAlreadyExistsException("conflict")))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.ALREADY_EXISTS.getCode());

    assertThat(toProto(new AccessControlException("not allowed")))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.PERMISSION_DENIED.getCode());

    assertThat(toProto(new NessieNamespaceNotFoundException(CONTENT_ERROR_DETAILS, "namespace not found")))
      .isInstanceOf(StatusRuntimeException.class)
      .hasCauseInstanceOf(NessieNamespaceNotFoundException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.NOT_FOUND.getCode());

    assertThat(toProto(new NessieNamespaceNotEmptyException(CONTENT_ERROR_DETAILS, "namespace not empty")))
      .isInstanceOf(StatusRuntimeException.class)
      .hasCauseInstanceOf(NessieNamespaceNotEmptyException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.ALREADY_EXISTS.getCode());

    assertThat(toProto(new NessieNamespaceAlreadyExistsException(CONTENT_ERROR_DETAILS, "namespace already exists")))
      .isInstanceOf(StatusRuntimeException.class)
      .hasCauseInstanceOf(NessieNamespaceAlreadyExistsException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.ALREADY_EXISTS.getCode());

    assertThat(
      toProto(
        new ConstraintViolationException(Collections.emptySet()) {
          @Override
          public Set<ConstraintViolation<?>> getConstraintViolations() {
            return super.getConstraintViolations();
          }
        }))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.INVALID_ARGUMENT.getCode());

    // everything else maps to INTERNAL
    assertThat(toProto(new NullPointerException()))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.INTERNAL.getCode());
  }

  @Test
  public void handlingNessieErrorDetails() {
    ReferenceConflicts conflicts = ImmutableReferenceConflicts.builder()
      .addConflicts(Conflict.conflict(Conflict.ConflictType.KEY_CONFLICT, ContentKey.of("test1"), "msg1"))
      .addConflicts(Conflict.conflict(Conflict.ConflictType.NAMESPACE_NOT_EMPTY, ContentKey.of("test2"), "msg2"))
        .build();

    assertThatThrownBy(() -> handle(() ->  {
      throw toProto(new NessieReferenceConflictException(conflicts, "exc-msg1", new RuntimeException("test")));
    }))
      .isInstanceOf(NessieReferenceConflictException.class)
      .hasMessage("exc-msg1")
      .asInstanceOf(InstanceOfAssertFactories.type(NessieReferenceConflictException.class))
      .extracting(NessieReferenceConflictException::getErrorDetails)
      .isEqualTo(conflicts);
  }

  @Test
  public void handlingLegacyExceptions() {
    // Validate the handling of exception data provided by old clients (without gRPC "trailers")
    assertThatThrownBy(() -> handleNessieNotFoundEx(() ->  {
      throw Status.NOT_FOUND
        .withDescription(ErrorCode.NAMESPACE_NOT_FOUND.name())
        .augmentDescription("Namespace ABC not found")
        .asRuntimeException();
    }))
      .isInstanceOf(NessieNamespaceNotFoundException.class)
      .hasMessage("Namespace ABC not found");

    assertThatThrownBy(() -> handleNessieNotFoundEx(() ->  {
      throw Status.INVALID_ARGUMENT
        .withDescription("test-msg123")
        .withCause(new RuntimeException("test-cause"))
        .asRuntimeException();
    }))
      .isInstanceOf(NessieBadRequestException.class)
      .hasMessageContaining("test-msg123");
  }

  @Test
  public void handlingExceptions() {
    NessieNotFoundException notFound = new NessieReferenceNotFoundException("not found");
    assertThatThrownBy(() -> handle(() ->  {
      throw toProto(notFound);
    }))
      .isInstanceOf(NessieNotFoundException.class)
      .hasMessage(notFound.getMessage());

    NessieNamespaceNotFoundException namespaceNotFound = new NessieNamespaceNotFoundException(
      CONTENT_ERROR_DETAILS, "not found");
    assertThatThrownBy(() -> handle(() ->  {
      throw toProto(namespaceNotFound);
    }))
      .isInstanceOf(NessieNamespaceNotFoundException.class)
      .hasMessage(namespaceNotFound.getMessage())
      .extracting("ErrorDetails")
      .isEqualTo(CONTENT_ERROR_DETAILS);

    NessieNamespaceNotEmptyException namespaceNotEmpty = new NessieNamespaceNotEmptyException(
      CONTENT_ERROR_DETAILS, "not empty");
    assertThatThrownBy(() -> handle(() ->  {
      throw toProto(namespaceNotEmpty);
    }))
      .isInstanceOf(NessieNamespaceNotEmptyException.class)
      .hasMessage(namespaceNotEmpty.getMessage())
      .extracting("ErrorDetails")
      .isEqualTo(CONTENT_ERROR_DETAILS);

    NessieNamespaceAlreadyExistsException namespaceAlreadyExists = new NessieNamespaceAlreadyExistsException(
      CONTENT_ERROR_DETAILS, "already exists");
    assertThatThrownBy(() -> handle(() ->  {
      throw toProto(namespaceAlreadyExists);
    }))
      .isInstanceOf(NessieNamespaceAlreadyExistsException.class)
      .hasMessage(namespaceAlreadyExists.getMessage())
      .extracting("ErrorDetails")
      .isEqualTo(CONTENT_ERROR_DETAILS);

    IllegalArgumentException iae = new IllegalArgumentException("illegal");
    assertThatThrownBy(() -> handle(() ->  {
      throw toProto(iae);
    }))
      .isInstanceOf(NessieBadRequestException.class)
      .hasMessageContaining(iae.getMessage());

    // any other exception will result in a StatusRuntimeException
    assertThatThrownBy(() -> handle(() ->  {
      throw toProto(new NessieBadRequestException(ImmutableNessieError.builder().message("x").errorCode(
        ErrorCode.BAD_REQUEST).status(400).reason("bad request").build()));
    })).isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  public void handlingNotFoundExceptions() {
    NessieContentNotFoundException notFound = new NessieContentNotFoundException(CONTENT_KEY, "not found");
    assertThatThrownBy(() -> handleNessieNotFoundEx(() ->  {
      throw toProto(notFound);
    }))
      .isInstanceOf(NessieContentNotFoundException.class)
      .hasMessage(notFound.getMessage())
      .extracting("ErrorDetails")
      .isEqualTo(CONTENT_ERROR_DETAILS);

    NessieNamespaceNotFoundException namespaceNotFound = new NessieNamespaceNotFoundException(
      CONTENT_ERROR_DETAILS, "not found");
    assertThatThrownBy(() -> handleNessieNotFoundEx(() ->  {
      throw toProto(namespaceNotFound);
    }))
      .isInstanceOf(NessieNamespaceNotFoundException.class)
      .hasMessage(namespaceNotFound.getMessage())
      .extracting("ErrorDetails")
      .isEqualTo(CONTENT_ERROR_DETAILS);

    IllegalArgumentException iae = new IllegalArgumentException("illegal");
    assertThatThrownBy(() -> handleNessieNotFoundEx(() ->  {
      throw toProto(iae);
    }))
      .isInstanceOf(NessieBadRequestException.class)
      .hasMessageContaining(iae.getMessage());

    // any other exception will result in a StatusRuntimeException
    assertThatThrownBy(() -> handleNessieNotFoundEx(() ->  {
      throw toProto(new NessieBadRequestException(ImmutableNessieError.builder().message("x").errorCode(
        ErrorCode.BAD_REQUEST).status(400).reason("bad request").build()));
    })).isInstanceOf(StatusRuntimeException.class);
  }
}
