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
package com.dremio.service.namespace.capabilities;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Objects;

/**
 * Describes the value of a capability for a particular source.
 *
 * @param <T> The boxed type of the value.
 * @param <X> The type of the capability this value references.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = BooleanCapabilityValue.class, name = "boolean"),
  @JsonSubTypes.Type(value = DoubleCapabilityValue.class, name = "double"),
  @JsonSubTypes.Type(value = StringCapabilityValue.class, name = "string"),
  @JsonSubTypes.Type(value = LongCapabilityValue.class, name = "long")
})
public class CapabilityValue<T, X extends Capability<T>> {

  private final X capability;
  private final T value;

  public CapabilityValue(X capability, T value) {
    super();
    this.capability = capability;
    this.value = value;
  }

  public Capability<T> getCapability() {
    return capability;
  }

  public T getValue() {
    return value;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other.getClass().equals(other.getClass()))) {
      return false;
    }
    CapabilityValue<T, X> castOther = (CapabilityValue<T, X>) other;
    return Objects.equal(capability, castOther.capability) && Objects.equal(value, castOther.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(capability, value);
  }



}
