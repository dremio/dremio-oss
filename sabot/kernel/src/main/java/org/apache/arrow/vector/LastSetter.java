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
package org.apache.arrow.vector;

import java.lang.reflect.Field;

import com.google.common.base.Throwables;
import org.apache.arrow.vector.complex.ListVector;

/**
 * Expose internals of NullableVarCharVector and NullableVarBinaryVector so that
 * we set the right value for lastSet such that calling setValueCount() doesn't
 * blow away vector if we've previously loaded it (as opposed to populated it locally).
 *
 */
public class LastSetter {
  public static void set(NullableVarCharVector v, int value) {
    try {
      Field f = v.getMutator().getClass().getDeclaredField("lastSet");
      f.setAccessible(true);
      f.set(v.getMutator(), value);
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  public static void set(NullableVarBinaryVector v, int value) {
    try {
      Field f = v.getMutator().getClass().getDeclaredField("lastSet");
      f.setAccessible(true);
      f.set(v.getMutator(), value);
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  public static void set(ListVector v, int value) {
    try {
      Field f = v.getClass().getDeclaredField("lastSet");
      f.setAccessible(true);
      f.set(v, value);
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }
}
