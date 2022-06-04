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

import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.toProto;
import static org.assertj.core.api.Assertions.assertThat;

import java.security.AccessControlException;
import java.util.Collections;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;

import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Tests for the {@link GrpcExceptionMapper}
 */
public class TestGrpcExceptionMapper {

  @Test
  public void exceptionToProtoConversion() {
    assertThat(toProto(new NessieNotFoundException("not found")))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.NOT_FOUND.getCode());

    assertThat(toProto(new NessieConflictException("conflict")))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.ALREADY_EXISTS.getCode());

    assertThat(toProto(new IllegalArgumentException("x")))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.INVALID_ARGUMENT.getCode());

    assertThat(toProto(new AccessControlException("not allowed")))
      .isInstanceOf(StatusRuntimeException.class)
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.PERMISSION_DENIED.getCode());

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
}
