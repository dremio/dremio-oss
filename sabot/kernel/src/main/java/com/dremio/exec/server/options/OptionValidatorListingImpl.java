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

import com.dremio.common.exceptions.UserException;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.options.OptionValidator;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.Options;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class OptionValidatorListingImpl implements OptionValidatorListing {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(OptionValidatorListingImpl.class);

  private final CaseInsensitiveMap<OptionValidator> validators;

  @Inject
  public OptionValidatorListingImpl(ScanResult scanResult) {
    this(getValidators(scanResult));
  }

  public OptionValidatorListingImpl(CaseInsensitiveMap<OptionValidator> validators) {
    this.validators = validators;
  }

  private static CaseInsensitiveMap<OptionValidator> getValidators(ScanResult scanResult) {
    ImmutableMap.Builder<String, OptionValidator> builder = ImmutableMap.builder();
    for (Class<?> clazz : scanResult.getAnnotatedClasses(Options.class)) {
      for (Field field : clazz.getDeclaredFields()) {
        if (!OptionValidator.class.isAssignableFrom(field.getType())) {
          continue;
        }

        try {
          final OptionValidator optionValidator = (OptionValidator) field.get(null);
          builder.put(optionValidator.getOptionName(), optionValidator);
        } catch (IllegalAccessException e) {
          logger.info("Ignoring not-accessible option {}.{}", clazz.getName(), field.getName(), e);
        }
      }
    }

    return CaseInsensitiveMap.newImmutableMap(builder.build());
  }

  @Override
  public OptionValidator getValidator(String name) {
    final OptionValidator validator = validators.get(name);
    if (validator == null) {
      throw UserException.validationError()
          .message(String.format("The option '%s' does not exist.", name))
          .build(logger);
    }
    return validator;
  }

  @Override
  public boolean isValid(String name) {
    return validators.containsKey(name);
  }

  @Override
  public Collection<OptionValidator> getValidatorList() {
    return validators.values();
  }

  @Override
  public Iterator<? extends OptionValidator> iterator() {
    return getValidatorList().iterator();
  }
}
