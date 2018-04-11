/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.service.accelerator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nullable;

import com.dremio.datastore.indexed.IndexKey;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * A builder used to generate RSQL filter string for testing
 */
public class FilterStringBuilder {
  private static final String ALL = "*";
  private static final Operator EQ = new Operator("==");
  private static final Operator CONTAINS = new Operator("=contains=");

  private final Multimap<OperatorBinding, String> filters = HashMultimap.create();

  public FilterStringBuilder addFilter(final IndexKey key, final String ...values) {
    return addFilter(key.getShortName(), EQ, values);
  }

  public FilterStringBuilder addContains(final String value) {
    return addFilter(ALL, CONTAINS, value);
  }

  private FilterStringBuilder addFilter(final String key, final Operator operator, final String ...values) {
    filters.putAll(OperatorBinding.of(key, operator), Arrays.asList(values));
    return this;
  }

  public String asRSQL() {
    return Joiner.on(";").join(
        FluentIterable
          .from(filters.asMap().keySet())
          .transform(new Function<OperatorBinding, String>() {
            @Nullable
            @Override
            public String apply(@Nullable final OperatorBinding binding) {
              final Collection<String> values = filters.get(binding);
              return Joiner.on(",").join(
                  FluentIterable
                    .from(values)
                    .transform(new Function<String, String>() {
                      @Nullable
                      @Override
                      public String apply(@Nullable final String value) {
                        return String.format("%s%s", binding, value);
                      }
                    })
              );
            }
          })
    );
  }
  private static final class OperatorBinding {
    private final String name;
    private final Operator operator;

    private OperatorBinding(final String name, final Operator operator) {
      this.name = name;
      this.operator = operator;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final OperatorBinding that = (OperatorBinding) o;
      return Objects.equals(operator, that.operator) &&
          Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(operator, name);
    }

    @Override
    public String toString() {
      return String.format("%s%s", name, operator);
    }

    public static OperatorBinding of(final String name, final Operator operator) {
      return new OperatorBinding(name, operator);
    }
  }

  private static final class Operator {

    private final String name;

    public Operator(final String name) {
      this.name = Preconditions.checkNotNull(name, "name is required");
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj instanceof Operator) {
        return Objects.equals(name, ((Operator) obj).name);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name);
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
