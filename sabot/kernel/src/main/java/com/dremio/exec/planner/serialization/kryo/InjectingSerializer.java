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


import java.lang.reflect.Field;
import java.util.Arrays;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;

/**
 * A reflective field serializer with injection capabilities.
 *
 * The serializer does not write subclasses of given injections but rather plugs them in during deserialization.
 * This is especially handy for system or component-wide singletons.
 *
 * Injections are provided via {@link InjectionMapping}.
 *
 * @param <T> type to serialize
 */
public class InjectingSerializer<T> extends FieldSerializer<T> {
  private static final Logger logger = LoggerFactory.getLogger(InjectingSerializer.class);

  private final InjectionMapping mapping;

  public InjectingSerializer(final Kryo kryo, final Class type, final InjectionMapping mapping) {
    super(kryo, type);
    this.mapping = Preconditions.checkNotNull(mapping, "injection mapping is required");
    config.setIgnoreSyntheticFields(true);
    config.setFieldsAsAccessible(true);
    transformFields();
  }

  @Override
  protected void rebuildCachedFields(final boolean minorRebuild) {
    super.rebuildCachedFields(minorRebuild);
    transformFields();
  }

  protected void transformFields() {
    if (mapping == null) {
      return;
    }

    final CachedField[] newFields = transform(getFields());
    setSuperField("fields", newFields);

    final CachedField[] newTransientFields = transform(getTransientFields());
    setSuperField("transientFields", newTransientFields);
  }

  protected void setSuperField(final String fieldName, final Object value) {
    try {
      final Field field = getClass().getSuperclass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(this, value);
    } catch (final NoSuchFieldException|IllegalAccessException e) {
      logger.error("unable to retrieve fields variable from FieldSerializer", e);
    }
  }

  protected CachedField[] transform(final CachedField[] fields) {
    return FluentIterable
        .from(Arrays.asList(fields))
        .transform(new Function<CachedField, CachedField>() {
          @Nullable
          @Override
          public CachedField apply(@Nullable final CachedField field) {
            final CachedField newField = mapping.findInjection(field.getField().getType())
                .transform(new Function<Injection, CachedField>() {
                  @Nullable
                  @Override
                  public CachedField apply(@Nullable final Injection injection) {
                    return new InjectingCachedField(field, injection.getValue());
                  }
                })
                .or(field);

            return newField;
          }
        })
        .toArray(CachedField.class);
  }

  protected static class DelegatingCachedField extends CachedField {
    protected final CachedField delegate;

    public DelegatingCachedField(final CachedField delegate) {
      this.delegate = delegate;
    }

    @Override
    public void setClass(final Class valueClass) {
      delegate.setClass(valueClass);
    }

    @Override
    public void setClass(final Class valueClass, final Serializer serializer) {
      delegate.setClass(valueClass, serializer);
    }

    @Override
    public void setSerializer(final Serializer serializer) {
      delegate.setSerializer(serializer);
    }

    @Override
    public Serializer getSerializer() {
      return delegate.getSerializer();
    }

    @Override
    public void setCanBeNull(final boolean canBeNull) {
      delegate.setCanBeNull(canBeNull);
    }

    @Override
    public Field getField() {
      return delegate.getField();
    }

    @Override
    public String toString() {
      return delegate.toString();
    }

    @Override
    public void write(final Output output, final Object object) {
      delegate.write(output, object);
    }

    @Override
    public void read(final Input input, final Object object) {
      try {
        delegate.read(input, object);
      }catch (Exception ex) {
        throw ex;
      }
    }

    @Override
    public void copy(final Object original, final Object copy) {
      delegate.copy(original, copy);
    }
  }

  protected static class InjectingCachedField<T> extends DelegatingCachedField {
    private final T value;

    public InjectingCachedField(final CachedField delegate, final T value) {
      super(delegate);
      this.value = value;
    }

    @Override
    public void write(final Output output, final Object object) {
      // skip writing value
    }

    @Override
    public void read(final Input input, final Object object) {
      try {
        delegate.getField().set(object, value);
      } catch (IllegalAccessException e) {
        logger.error("unable to inject value", e);
      }
    }

    @Override
    public void copy(final Object original, final Object copy) {
      delegate.copy(original, copy);
    }
  }


}
