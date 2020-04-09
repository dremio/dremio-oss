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
package com.dremio.exec.store;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.types.TypeProtos.MajorType;

/**
 * Type Coercion interface to get the target type for a particular field
 * The coercion reader will cast the incoming data to this type
 */
public interface TypeCoercion {
  /**
   * Get the target type for the field
   * @param field
   * @return
   */
  MajorType getType(Field field);
}
