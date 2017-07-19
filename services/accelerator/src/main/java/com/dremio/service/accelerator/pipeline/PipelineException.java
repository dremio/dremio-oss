/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.accelerator.pipeline;

import javax.annotation.Nullable;

import com.dremio.service.accelerator.proto.Acceleration;
import com.google.common.base.Function;
import com.google.common.base.Optional;

/**
 * A generic exception thrown by {@link PipelineDriver}.
 */
public class PipelineException extends RuntimeException {
  private final Optional<Acceleration> acceleration;

  public PipelineException(final String message) {
    this(message, Optional.<Acceleration>absent());
  }

  public PipelineException(final String message, final Optional<Acceleration> acceleration) {
    this(message, acceleration, null);
  }

  public PipelineException(final String message, final Optional<Acceleration> acceleration, final Throwable cause) {
    super(message, cause);
    this.acceleration = acceleration;
  }

  private String getId() {
    return acceleration.transform(new Function<Acceleration, String>() {
      @Nullable
      @Override
      public String apply(@Nullable final Acceleration input) {
        return input.getId().getId();
      }
    }).or("[none]");
  }

  @Override
  public String getMessage() {
    return String.format("%s -- acceleration id: %s", super.getMessage(), getId());
  }

  public Optional<Acceleration> getAcceleration() {
    return acceleration;
  }
}
