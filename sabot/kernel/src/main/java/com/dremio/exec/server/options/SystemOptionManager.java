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
package com.dremio.exec.server.options;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.IteratorUtils;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.PassThroughSerializer;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StringSerializer;
import com.dremio.exec.serialization.JacksonSerializer;
import com.dremio.exec.server.options.OptionValue.OptionType;
import com.dremio.exec.store.sys.PersistentStore;
import com.dremio.exec.store.sys.PersistentStoreProvider;
import com.dremio.exec.store.sys.store.KVPersistentStore.PersistentStoreCreator;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * {@link OptionManager} that holds options within {@link com.dremio.exec.server.SabotContext}.
 * Only one instance of this class exists per node. Options set at the system level affect the entire system and
 * persist between restarts.
 */
public class SystemOptionManager extends BaseOptionManager implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemOptionManager.class);

  private static final String SYSTEM_OPTION_PREFIX = "dremio.debug.sysopt.";
  public static final String STORE_NAME = "sys.options";

  private static final Function<Entry<String, OptionValue>, OptionValue> EXTRACT_OPTIONS = new Function<Entry<String, OptionValue>, OptionValue>() {
    @Override
    public OptionValue apply(Entry<String, OptionValue> option) {
      return option.getValue();
    }
  };

  private final PersistentStoreProvider provider;
  private final LogicalPlanPersistence lpPersistance;
  private final CaseInsensitiveMap<OptionValidator> validators;

  /**
   * Persistent store for options that have been changed from default.
   * NOTE: CRUD operations must use lowercase keys.
   */
  private PersistentStore<OptionValue> options;



  public SystemOptionManager(ScanResult scanResult, LogicalPlanPersistence lpPersistence, final PersistentStoreProvider provider) {
    this.provider = provider;
    this.lpPersistance = lpPersistence;

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

  /**
   * Initializes this option manager.
   *
   * @return this option manager
   * @throws Exception
   */
  public SystemOptionManager init() throws Exception {
    options = provider.getOrCreateStore(STORE_NAME, OptionStoreCreator.class, new JacksonSerializer<>(lpPersistance.getMapper(), OptionValue.class));
    // if necessary, deprecate and replace options from persistent store
    for (final Entry<String, OptionValue> entry : Lists.newArrayList(options.getAll())) {
      final String name = entry.getKey();
      final OptionValue value = entry.getValue();
      final OptionValidator validator = validators.get(name);
      if (validator == null) {
        // deprecated option, delete.
        options.delete(name);
        logger.warn("Deleting deprecated option `{}`", name);
      } else {
        final String canonicalName = validator.getOptionName().toLowerCase();
        if (!name.equals(canonicalName)) {
          // for backwards compatibility <= 1.1, rename to lower case.
          logger.warn("Changing option name to lower case `{}`", name);
          options.delete(name);
          options.put(canonicalName, value);
        }
      }
    }

    updateBasedOnSystemProperties();

    return this;
  }

  public static class OptionStoreCreator implements PersistentStoreCreator {
    @Override
    public KVStore<String, byte[]> build(StoreBuildingFactory factory) {
      return factory.<String, byte[]>newStore()
        .name(STORE_NAME)
        .keySerializer(StringSerializer.class)
        .valueSerializer(PassThroughSerializer.class)
        .build();
    }
  }

  /**
   * Gets the {@link OptionValidator} associated with the name.
   *
   * @param name name of the option
   * @return the associated validator
   * @throws UserException - if the validator is not found
   */
  @Override
  public OptionValidator getValidator(final String name) {
    final OptionValidator validator = validators.get(name);
    if (validator == null) {
      throw UserException.validationError()
          .message(String.format("The option '%s' does not exist.", name))
          .build(logger);
    }
    return validator;
  }


  public boolean isValid(String name){
    return validators.containsKey(name);
  }

  public boolean isSet(String name){
    return options.get(name) != null;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    final Map<String, OptionValue> buildList = CaseInsensitiveMap.newHashMap();
    // populate the default options
    for (final Map.Entry<String, OptionValidator> entry : validators.entrySet()) {
      buildList.put(entry.getKey(), entry.getValue().getDefault());
    }
    // override if changed
    for (final Map.Entry<String, OptionValue> entry : Lists.newArrayList(options.getAll())) {
      buildList.put(entry.getKey(), entry.getValue());
    }
    return buildList.values().iterator();
  }

  @Override
  public OptionValue getOption(final String name) {
    // check local space (persistent store)
    final OptionValue value = options.get(name);
    if (value != null) {
      return value;
    }

    // otherwise, return default.
    final OptionValidator validator = getValidator(name);
    return validator.getDefault();
  }

  @Override
  public void setOption(final OptionValue value) {
    checkArgument(value.type == OptionType.SYSTEM, "OptionType must be SYSTEM.");
    final String name = value.name.toLowerCase();
    final OptionValidator validator = getValidator(name);

    validator.validate(value); // validate the option

    if (options.get(name) == null && value.equals(validator.getDefault())) {
      return; // if the option is not overridden, ignore setting option to default
    }
    options.put(name, value);
  }

  @Override
  public void deleteOption(final String name, OptionType type) {
    checkArgument(type == OptionType.SYSTEM, "OptionType must be SYSTEM.");

    getValidator(name); // ensure option exists
    options.delete(name.toLowerCase());
  }

  @Override
  public void deleteAllOptions(OptionType type) {
    checkArgument(type == OptionType.SYSTEM, "OptionType must be SYSTEM.");
    final Set<String> names = Sets.newHashSet();
    for (final Map.Entry<String, OptionValue> entry : Lists.newArrayList(options.getAll())) {
      names.add(entry.getKey());
    }
    for (final String name : names) {
      options.delete(name); // should be lowercase
    }
  }

  @Override
  public OptionList getOptionList() {
    return (OptionList) IteratorUtils.toList(iterator());
  }

  /**
   * @return all system options that have been set to a non-default value
   */
  public OptionList getNonDefaultOptions() {
    Iterator<OptionValue> persistedOptions = Iterators.transform(this.options.getAll(), EXTRACT_OPTIONS);

    OptionList nonDefaultOptions = new OptionList();
    Iterators.addAll(nonDefaultOptions, persistedOptions);
    return nonDefaultOptions;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(options);
  }


  private void updateBasedOnSystemProperties() {

    // get the system options property
    for(Entry<Object, Object> property : System.getProperties().entrySet()){
      String key = (String) property.getKey();
      String value = (String) property.getValue();

      if(!key.startsWith(SYSTEM_OPTION_PREFIX)){
        continue;
      }

      final String optionName = key.substring(SYSTEM_OPTION_PREFIX.length());

      OptionValidator validator = validators.get(optionName.toLowerCase());
      if(validator == null){
        logger.warn("Failure resolving system property of {}. No property with this name found.", optionName);
        continue;
      }

      try{
        switch(validator.getDefault().kind){
        case BOOLEAN:
          this.setOption(OptionValue.createBoolean(OptionType.SYSTEM, validator.getOptionName(), Boolean.parseBoolean(value)));
          break;
        case DOUBLE:
          this.setOption(OptionValue.createDouble(OptionType.SYSTEM, validator.getOptionName(), Double.parseDouble(value)));
          break;
        case LONG:
          this.setOption(OptionValue.createLong(OptionType.SYSTEM, validator.getOptionName(), Long.parseLong(value)));
          break;
        case STRING:
          this.setOption(OptionValue.createString(OptionType.SYSTEM, validator.getOptionName(), value));
          break;
        default:
          break;

        }

        logger.info("Updated system property {} to value of {}.", optionName, property.getValue());

      } catch (RuntimeException e) {
        logger.warn("Failure setting property of system property {} to value of {}.", optionName, property.getValue(), e);
      }
    }

  }

  public int settingCount(){
    return validators.size();
  }

}
