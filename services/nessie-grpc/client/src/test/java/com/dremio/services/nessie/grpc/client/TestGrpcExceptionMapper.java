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
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handleNamespaceCreation;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handleNamespaceDeletion;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handleNamespaceRetrieval;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handleNessieNotFoundEx;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.toProto;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.AccessControlException;
import java.util.Collections;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;

import org.junit.jupiter.api.Test;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Tests for the {@link GrpcExceptionMapper}
 */
public class TestGrpcExceptionMapper {

  @Test
  public void exceptionToProtoConversion() {
    assertThat(toProto(new IllegalArgumentException("x")))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.INVALID_ARGUMENT.getCode());

    assertThat(toProto(new NessieNotFoundException("not found")))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.NOT_FOUND.getCode());

    assertThat(toProto(new NessieConflictException("conflict")))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.ALREADY_EXISTS.getCode());

    assertThat(toProto(new AccessControlException("not allowed")))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.PERMISSION_DENIED.getCode());

    assertThat(toProto(new NessieNamespaceNotFoundException("namespace not found")))
      .isInstanceOf(StatusRuntimeException.class)
      .hasCauseInstanceOf(NessieNamespaceNotFoundException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.NOT_FOUND.getCode());

    assertThat(toProto(new NessieNamespaceNotEmptyException("namespace not empty")))
      .isInstanceOf(StatusRuntimeException.class)
      .hasCauseInstanceOf(NessieNamespaceNotEmptyException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.ALREADY_EXISTS.getCode());

    assertThat(toProto(new NessieNamespaceAlreadyExistsException("namespace already exists")))
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
  public void handlingExceptions() {
    NessieNotFoundException notFound = new NessieNotFoundException("not found");
    assertThatThrownBy(() -> handle(() ->  {
      throw toProto(notFound);
    }))
      .isInstanceOf(NessieNotFoundException.class)
      .hasMessage(notFound.getMessage());

    NessieNamespaceNotFoundException namespaceNotFound = new NessieNamespaceNotFoundException("not found");
    assertThatThrownBy(() -> handle(() ->  {
      throw toProto(namespaceNotFound);
    }))
      .isInstanceOf(NessieNamespaceNotFoundException.class)
      .hasMessage(namespaceNotFound.getMessage());

    NessieNamespaceNotEmptyException namespaceNotEmpty = new NessieNamespaceNotEmptyException("not empty");
    assertThatThrownBy(() -> handle(() ->  {
      throw toProto(namespaceNotEmpty);
    }))
      .isInstanceOf(NessieNamespaceNotEmptyException.class)
      .hasMessage(namespaceNotEmpty.getMessage());

    NessieNamespaceAlreadyExistsException namespaceAlreadyExists = new NessieNamespaceAlreadyExistsException("already exists");
    assertThatThrownBy(() -> handle(() ->  {
      throw toProto(namespaceAlreadyExists);
    }))
      .isInstanceOf(NessieNamespaceAlreadyExistsException.class)
      .hasMessage(namespaceAlreadyExists.getMessage());

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
    NessieNotFoundException notFound = new NessieNotFoundException("not found");
    assertThatThrownBy(() -> handleNessieNotFoundEx(() ->  {
      throw toProto(notFound);
    }))
      .isInstanceOf(NessieNotFoundException.class)
      .hasMessage(notFound.getMessage());

    NessieNamespaceNotFoundException namespaceNotFound = new NessieNamespaceNotFoundException("not found");
    assertThatThrownBy(() -> handleNessieNotFoundEx(() ->  {
      throw toProto(namespaceNotFound);
    }))
      .isInstanceOf(NessieNamespaceNotFoundException.class)
      .hasMessage(namespaceNotFound.getMessage());

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

  @Test
  public void handlingNamespaceCreationExceptions() {
    NessieNamespaceAlreadyExistsException exists = new NessieNamespaceAlreadyExistsException("namespace already exists");

    assertThatThrownBy(() -> handleNamespaceCreation(() ->  {
      throw toProto(exists);
    }))
      .isInstanceOf(NessieNamespaceAlreadyExistsException.class)
      .hasMessage(exists.getMessage());

    NessieReferenceNotFoundException refNotFound = new NessieReferenceNotFoundException("ref not found");
    assertThatThrownBy(() -> handleNamespaceCreation(() ->  {
      throw toProto(refNotFound);
    }))
      .isInstanceOf(NessieReferenceNotFoundException.class)
      .hasMessage(refNotFound.getMessage());

    // any other exception will result in a StatusRuntimeException
    assertThatThrownBy(() -> handleNamespaceCreation(() ->  {
      throw toProto(new NessieNamespaceNotEmptyException("x"));
    })).isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  public void handlingNamespaceDeletionExceptions() {
    NessieNamespaceNotFoundException namespaceNotFound = new NessieNamespaceNotFoundException("namespace not found");
    assertThatThrownBy(() -> handleNamespaceDeletion(() ->  {
      throw toProto(namespaceNotFound);
    }))
      .isInstanceOf(NessieNamespaceNotFoundException.class)
      .hasMessage(namespaceNotFound.getMessage());

    NessieNamespaceNotEmptyException namespaceNotEmpty = new NessieNamespaceNotEmptyException("namespace not empty");
    assertThatThrownBy(() -> handleNamespaceDeletion(() ->  {
      throw toProto(namespaceNotEmpty);
    }))
      .isInstanceOf(NessieNamespaceNotEmptyException.class)
      .hasMessage(namespaceNotEmpty.getMessage());

    NessieReferenceNotFoundException refNotFound = new NessieReferenceNotFoundException("ref not found");
    assertThatThrownBy(() -> handleNamespaceDeletion(() ->  {
      throw toProto(refNotFound);
    }))
      .isInstanceOf(NessieReferenceNotFoundException.class)
      .hasMessage(refNotFound.getMessage());

    // any other exception will result in a StatusRuntimeException
    assertThatThrownBy(() -> handleNamespaceDeletion(() ->  {
      throw toProto(new NessieNamespaceAlreadyExistsException("x"));
    })).isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  public void handlingNamespaceRetrievalExceptions() {
    NessieNamespaceNotFoundException namespaceNotFound = new NessieNamespaceNotFoundException("namespace not found");
    assertThatThrownBy(() -> handleNamespaceRetrieval(() ->  {
      throw toProto(namespaceNotFound);
    }))
      .isInstanceOf(NessieNamespaceNotFoundException.class)
      .hasMessage(namespaceNotFound.getMessage());

    NessieReferenceNotFoundException refNotFound = new NessieReferenceNotFoundException("ref not found");
    assertThatThrownBy(() -> handleNamespaceRetrieval(() ->  {
      throw toProto(refNotFound);
    }))
      .isInstanceOf(NessieReferenceNotFoundException.class)
      .hasMessage(refNotFound.getMessage());

    // any other exception will result in a StatusRuntimeException
    assertThatThrownBy(() -> handleNamespaceRetrieval(() ->  {
      throw toProto(new NessieNamespaceAlreadyExistsException("x"));
    })).isInstanceOf(StatusRuntimeException.class);
  }
}
