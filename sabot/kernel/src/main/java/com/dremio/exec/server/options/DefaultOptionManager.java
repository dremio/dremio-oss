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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.options.OptionList;
import com.dremio.options.OptionValidator;
import com.dremio.options.OptionValue;
import com.dremio.options.Options;
import com.google.common.collect.ImmutableMap;

/**
 * Option manager for static defaults.
 */
public class DefaultOptionManager extends BaseOptionManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultOptionManager.class);
  private final CaseInsensitiveMap<OptionValidator> validators;

  public DefaultOptionManager(ScanResult scanResult) {
    this.validators = getValidators(scanResult);
  }

  private static CaseInsensitiveMap<OptionValidator> getValidators(ScanResult scanResult) {
    ImmutableMap.Builder<String, OptionValidator> builder = ImmutableMap.builder();
    for(Class<?> clazz: scanResult.getAnnotatedClasses(Options.class)) {
      for(Field field: clazz.getDeclaredFields()) {
        if (!(OptionValidator.class.isAssignableFrom(field.getType()))) {
          continue;
        }

        final OptionValidator optionValidator;
        try {
          optionValidator = (OptionValidator) field.get(null);
        } catch (IllegalAccessException e) {
          logger.info("Ignoring not-accessible option {}.{}", clazz.getName(), field.getName(), e);
          continue;
        }
        builder.put(optionValidator.getOptionName(), optionValidator);
      }
    }

    return CaseInsensitiveMap.newImmutableMap(builder.build());
  }

  @Override
  public void setOption(OptionValue value) {
    throw new UnsupportedOperationException("Cannot set option in DefaultOptionManager");
  }

  @Override
  public void deleteOption(String name, OptionValue.OptionType type) {
    throw new UnsupportedOperationException("Cannot delete option in DefaultOptionManager");
  }

  @Override
  public void deleteAllOptions(OptionValue.OptionType type) {
    throw new UnsupportedOperationException("Cannot delete all options in DefaultOptionManager");
  }

  @Override
  public OptionValue getOption(String name) {
    final OptionValidator optionValidator = getValidator(name);
    return optionValidator.getDefault();
  }

  @Override
  public OptionList getOptionList() {
    final OptionList result = new OptionList();
    final Collection<OptionValue> optionValues = getOptions().values();
    result.addAll(optionValues);
    return result;
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
  public Iterator<OptionValue> iterator() {
    return getOptions().values().iterator();
  }

  Map<String, OptionValue> getOptions() {
    final Map<String, OptionValue> optionMap = CaseInsensitiveMap.newHashMap();

    // Unpack OptionValues from validators
    validators.forEach((key, value) -> optionMap.put(key, value.getDefault()));
    return optionMap;
  }

  public boolean isValid(String name) {
    return validators.containsKey(name);
  }

}
