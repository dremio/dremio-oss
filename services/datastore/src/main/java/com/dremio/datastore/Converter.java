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
package com.dremio.datastore;

/**
 * converts from T1 to T2 and back
 *
 * It is expected that revert(convert(v)).equals(v) if convert(v) does not throw an exception
 *
 * @param <T1>
 * @param <T2>
 */
public abstract class Converter<T1, T2> {

  /**
   * @param v the T1 value to convert to T2
   * @return v converted to T2
   */
  public abstract T2 convert(T1 v);

  /**
   * @param v the T2 value to turn back into T1
   * @return v converted back to T1
   */
  public abstract T1 revert(T2 v);

  /**
   * composes 2 converters
   * @param before
   * @return this o before
   */
  public <T0> Converter<T0, T2> compose(final Converter<T0, T1> before) {
    return new ComposedConverter<>(before, this);
  }

  /**
   * composes 2 converters
   * @param after
   * @return after o this
   */
  public <T3> Converter<T1, T3> andThen(Converter<T2, T3> after) {
    return new ComposedConverter<>(this, after);
  }

  private static final class ComposedConverter<A, B, C> extends Converter<A, C> {
    private final Converter<A, B> before;
    private final Converter<B, C> after;

    private ComposedConverter(Converter<A, B> before, Converter<B, C> after) {
      this.before = before;
      this.after = after;
    }

    @Override
    public C convert(A v) {
      return after.convert(before.convert(v));
    }

    @Override
    public A revert(C v) {
      return before.revert(after.revert(v));
    }
  }
}
