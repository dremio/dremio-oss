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
package com.dremio.exec.planner.serialization.kryo;

import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class InjectionMapping {
  private final Map<Class, Injection> mapping = Maps.newHashMap();

  protected InjectionMapping() { }

  public Optional<Injection> findInjection(final Class klazz) {
    Class current = klazz;
    while (current != null) {
      final Injection injection = mapping.get(current);
      if (injection != null) {
        return Optional.of(injection);
      }
      current = current.getSuperclass();
    }
    return Optional.absent();
  }

  private void addInjections(final Iterable<Injection> injections) {
    Preconditions.checkNotNull(injections, "injections are required");
    for (final Injection injection : injections) {
      mapping.put(injection.getType(), Preconditions.checkNotNull(injection, "injection is required"));
    }
  }

  public static InjectionMapping of(final Iterable<Injection> injections) {
    Preconditions.checkNotNull(injections, "injections are required");

    final InjectionMapping mapping = new InjectionMapping();
    mapping.addInjections(injections);
    return mapping;
  }
}
