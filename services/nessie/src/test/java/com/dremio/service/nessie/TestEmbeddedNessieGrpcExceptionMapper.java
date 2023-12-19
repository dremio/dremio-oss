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
package com.dremio.service.nessie;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Path;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.dremio.services.nessie.grpc.GrpcExceptionMapper;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

class TestEmbeddedNessieGrpcExceptionMapper {
  @Test
  void handlerLoadAsService() {
    // Note EmbeddedNessieGrpcExceptionMapper is loaded by GrpcExceptionMapper automatically
    assertThat(
      GrpcExceptionMapper.toProto(new ConstraintViolationException("test-msg", Collections.emptySet())))
      .isInstanceOf(StatusRuntimeException.class)
      .hasMessageContaining("test-msg")
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.INVALID_ARGUMENT.getCode());

  }
  @Test
  void validationExceptionToProto() {
    Path path = Mockito.mock(Path.class);
    Mockito.when(path.toString()).thenReturn("path123");
    ConstraintViolation<?> violation = Mockito.mock(ConstraintViolation.class);
    Mockito.when(violation.getMessage()).thenReturn("test-msg1");
    Mockito.when(violation.getPropertyPath()).thenReturn(path);

    assertThat(
      new EmbeddedNessieGrpcExceptionMapper().toProto(new ConstraintViolationException(
        Collections.singleton(violation))))
      .isInstanceOf(StatusRuntimeException.class)
      .hasMessageContaining("test-msg1")
      .hasMessageContaining("path123")
      .extracting(e -> e.getStatus().getCode())
      .isEqualTo(Status.INVALID_ARGUMENT.getCode());
  }
}
