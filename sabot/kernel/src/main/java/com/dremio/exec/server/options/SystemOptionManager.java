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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Set;

import javax.inject.Provider;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.exec.exception.StoreException;
import com.dremio.exec.serialization.JacksonSerializer;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidator;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.options.OptionValueProto;
import com.dremio.service.Service;
import com.google.common.collect.Sets;

/**
 * {@link OptionManager} that holds options.  Only one instance of this class exists per node. Options set at the system
 * level affect the entire system and persist between restarts.
 */
public class SystemOptionManager extends BaseOptionManager implements Service, ProjectOptionManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemOptionManager.class);

  private static final String SYSTEM_OPTION_PREFIX = "dremio.debug.sysopt.";
  public static final String STORE_NAME = "options";
  public static final String LEGACY_STORE_NAME = "sys.options";

  private final OptionValidatorListing optionValidatorListing;
  private final LogicalPlanPersistence lpPersistance;
  private final Provider<LegacyKVStoreProvider> storeProvider;
  private final boolean inMemory;

  /**
   * Persistent store for options that have been changed from default.
   * NOTE: CRUD operations must use lowercase keys.
   */
  private LegacyKVStore<String, OptionValueProto> options;

  public SystemOptionManager(OptionValidatorListing optionValidatorListing,
                             LogicalPlanPersistence lpPersistence,
                             final Provider<LegacyKVStoreProvider> storeProvider,
                             boolean inMemory) {
    super(optionValidatorListing);
    this.optionValidatorListing = optionValidatorListing;
    this.lpPersistance = lpPersistence;
    this.storeProvider = storeProvider;
    this.inMemory = inMemory;
  }

  /**
   * Initializes this option manager.
   *
   * @return this option manager
   * @throws Exception
   */
  @Override
  public void start() throws Exception {
    options = inMemory ? new InMemoryLocalStore<>() : storeProvider.get().getStore(OptionProtoStoreCreator.class);
    migrateLegacyOptions();
    updateBasedOnSystemProperties();
  }

  private void migrateLegacyOptions() throws StoreException {
    if (inMemory) {
      return; // In-memory store does not start with any options
    }
    final OptionValueStore legacyOptions = getLegacyStore();
    legacyOptions.getAll().forEachRemaining(
      entry -> {
        final String name = entry.getKey();
        final OptionValue value = entry.getValue();

        try {
          final OptionValidator validator = optionValidatorListing.getValidator(name);
          final String canonicalName = validator.getOptionName().toLowerCase(Locale.ROOT);
          if (!name.equals(canonicalName)) {
            // for backwards compatibility <= 1.1, rename to lower case.
            logger.warn("Changing option name to lower case `{}`", name);
          }
          legacyOptions.delete(name);
          options.put(canonicalName, OptionValueProtoUtils.toOptionValueProto(value));
        } catch (UserException e) {
          legacyOptions.delete(name);
          logger.warn("Deleting deprecated option `{}`", name);
        }
      }
    );
  }

  private OptionValueStore getLegacyStore() throws StoreException {
    final OptionValueStore store = new OptionValueStore(
      storeProvider,
      OptionStoreCreator.class,
      new JacksonSerializer<>(lpPersistance.getMapper(), OptionValue.class)
    );
    try {
      store.start();
    } catch (Exception e) {
      throw new StoreException(String.format("Unable to get persistent store %s", OptionStoreCreator.class.getName()), e);
    }
    return store;
  }

  public static class OptionStoreCreator implements OptionValueStore.OptionValueStoreCreator {
    @Override
    public LegacyKVStore<String, byte[]> build(LegacyStoreBuildingFactory factory) {
      return factory.<String, byte[]>newStore()
        .name(LEGACY_STORE_NAME)
        .keyFormat(Format.ofString())
        .valueFormat(Format.ofBytes())
        .build();
    }
  }

  public static class OptionProtoStoreCreator implements LegacyKVStoreCreationFunction<String, OptionValueProto> {
    @Override
    public LegacyKVStore<String, OptionValueProto> build(LegacyStoreBuildingFactory factory) {
      return factory.<String, OptionValueProto>newStore()
        .name(STORE_NAME)
        .keyFormat(Format.ofString())
        .valueFormat(Format.ofProtobuf(OptionValueProto.class))
        .build();
    }
  }

  @Override
  public boolean isValid(String name){
    return optionValidatorListing.isValid(name);
  }

  @Override
  public boolean isSet(String name){
    return options.get(name.toLowerCase(Locale.ROOT)) != null;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    OptionList optionList = new OptionList();
    options.find().forEach(
      entry -> optionList.add(OptionValueProtoUtils.toOptionValue(entry.getValue()))
    );
    return optionList.iterator();
  }

  @Override
  public OptionValue getOption(final String name) {
    // check local space (persistent store)
    final OptionValueProto value = options.get(name.toLowerCase(Locale.ROOT));
    if (value != null) {
      return OptionValueProtoUtils.toOptionValue(value);
    }
    return null;
  }

  @Override
  public boolean setOption(final OptionValue value) {
    checkArgument(value.getType() == OptionType.SYSTEM, "OptionType must be SYSTEM.");
    final String name = value.getName().toLowerCase(Locale.ROOT);
    final OptionValidator validator = optionValidatorListing.getValidator(name);

    validator.validate(value); // validate the option

    if (options.get(name) == null && value.equals(validator.getDefault())) {
      return true; // no need to set option if value is the default value
    }
    options.put(name, OptionValueProtoUtils.toOptionValueProto(value));
    return true;
  }

  @Override
  public boolean deleteOption(final String name, OptionType type) {
    checkArgument(type == OptionType.SYSTEM, "OptionType must be SYSTEM.");

    optionValidatorListing.getValidator(name); // ensure option exists
    options.delete(name.toLowerCase(Locale.ROOT));
    return true;
  }

  @Override
  public boolean deleteAllOptions(OptionType type) {
    checkArgument(type == OptionType.SYSTEM, "OptionType must be SYSTEM.");
    final Set<String> names = Sets.newHashSet();
    options.find().forEach(
      entry -> names.add(entry.getKey())
    );
    for (final String name : names) {
      options.delete(name.toLowerCase(Locale.ROOT)); // should be lowercase
    }
    return true;
  }

  /**
   * @return all system options that have been set to a non-default value
   */
  @Override
  public OptionList getNonDefaultOptions() {
    OptionList nonDefaultOptions = new OptionList();
    options.find().forEach(
      entry -> nonDefaultOptions.add(OptionValueProtoUtils.toOptionValue(entry.getValue()))
    );
    return nonDefaultOptions;
  }

  @Override
  public void close() throws Exception {
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

      OptionValidator validator = optionValidatorListing.getValidator(optionName.toLowerCase(Locale.ROOT));
      if(validator == null){
        logger.warn("Failure resolving system property of {}. No property with this name found.", optionName);
        continue;
      }

      try{
        switch(validator.getDefault().getKind()){
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

  @Override
  protected boolean supportsOptionType(OptionType type) {
    return type == OptionType.SYSTEM;
  }
}
