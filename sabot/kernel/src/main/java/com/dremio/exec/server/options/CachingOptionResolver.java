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
package com.dremio.exec.server.options;

import com.dremio.options.OptionResolver;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/** A read-only OptionManager that lazily caches the response from the delegate OptionManager. */
public class CachingOptionResolver implements OptionResolver {

  private final OptionResolver delegate;

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  private final Cache<String, Object> options =
      CacheBuilder.newBuilder().maximumSize(10000).build();

  public CachingOptionResolver(OptionResolver delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean getOption(BooleanValidator validator) {
    Object object = options.getIfPresent(validator.getOptionName());
    if (null != object) {
      return (boolean) object;
    }
    boolean value = delegate.getOption(validator);
    options.put(validator.getOptionName(), value);
    return value;
  }

  @Override
  public double getOption(DoubleValidator validator) {
    Object object = options.getIfPresent(validator.getOptionName());
    if (null != object) {
      return (double) object;
    }
    double value = delegate.getOption(validator);
    options.put(validator.getOptionName(), value);
    return value;
  }

  @Override
  public long getOption(LongValidator validator) {
    Object object = options.getIfPresent(validator.getOptionName());
    if (null != object) {
      return (long) object;
    }
    long value = delegate.getOption(validator);
    options.put(validator.getOptionName(), value);
    return value;
  }

  @Override
  public String getOption(StringValidator validator) {
    Object object = options.getIfPresent(validator.getOptionName());
    if (null != object) {
      return (String) object;
    }
    String value = delegate.getOption(validator);
    options.put(validator.getOptionName(), value);
    return value;
  }
}
