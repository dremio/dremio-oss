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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Provider;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.exec.serialization.JacksonSerializer;
import com.dremio.options.OptionChangeListener;
import com.dremio.options.OptionChangeNotification;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidator;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.options.OptionValueProto;
import com.dremio.options.OptionValueProtoList;
import com.dremio.service.Pointer;
import com.dremio.service.Service;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.inject.util.Providers;

/**
 * {@link OptionManager} that holds options.  Only one instance of this class exists per node. Options set at the system
 * level affect the entire system and persist between restarts.
 */
public class SystemOptionManager extends BaseOptionManager implements Service, ProjectOptionManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemOptionManager.class);

  private static final String SYSTEM_OPTION_PREFIX = "dremio.debug.sysopt.";
  private static final String STORE_NAME = "project_options";
  private static final String LEGACY_JACKSON_STORE_NAME = "sys.options";
  private static final String LEGACY_PROTO_STORE_NAME = "options";
  static final String OPTIONS_KEY = "options";

  public static final String LOCAL_TASK_LEADER_NAME = "systemoptionpolling";
  private static final int FETCH_SYSTEM_OPTION_POLLING_FREQUENCY_MIN = 1;

  private final OptionValidatorListing optionValidatorListing;
  private final LogicalPlanPersistence lpPersistance;
  private final Provider<LegacyKVStoreProvider> storeProvider;
  private final Provider<SchedulerService> scheduler;
  private final OptionChangeBroadcaster broadcaster;
  private final boolean inMemory;
  private final Set<OptionChangeListener> listeners = Sets.newConcurrentHashSet();
  private volatile List<OptionValueProto> cachedOptionProtoList;
  private long cacheCalls;
  private long kvStoreCalls;

  /**
   * Persistent store for options that have been changed from default.
   * NOTE: CRUD operations must use lowercase keys.
   */
  private LegacyKVStore<String, OptionValueProtoList> options;

  public SystemOptionManager(
    OptionValidatorListing optionValidatorListing,
    LogicalPlanPersistence lpPersistence,
    final Provider<LegacyKVStoreProvider> storeProvider,
    boolean inMemory
  ) {
    this(optionValidatorListing, lpPersistence, storeProvider, Providers.of(null), null, inMemory);
  }

  public SystemOptionManager(
    OptionValidatorListing optionValidatorListing,
    LogicalPlanPersistence lpPersistence,
    final Provider<LegacyKVStoreProvider> storeProvider,
    Provider<SchedulerService> scheduler,
    OptionChangeBroadcaster broadcaster,
    boolean inMemory
  ) {
    super(optionValidatorListing);
    this.optionValidatorListing = optionValidatorListing;
    this.lpPersistance = lpPersistence;
    this.storeProvider = storeProvider;
    this.inMemory = inMemory;
    this.scheduler = scheduler;
    this.broadcaster = broadcaster;
    cachedOptionProtoList = Collections.emptyList();
    cacheCalls = 0;
    kvStoreCalls = 0;
  }

  /**
   * Initializes this option manager.
   */
  @Override
  public void start() throws Exception {
    options = inMemory ? new InMemoryLocalStore<>() : storeProvider.get().getStore(OptionStoreCreator.class);
    migrateLegacyOptions();

    populateCache(); // Start tasks need to access options
    updateBasedOnSystemProperties();
    filterInvalidOptions();
    populateCache();
    if (scheduler.get() != null) {
      scheduler.get().schedule(Schedule.Builder.everyMinutes(FETCH_SYSTEM_OPTION_POLLING_FREQUENCY_MIN).build(),
        new FetchSystemOptionTask());
    }
  }

  private void filterInvalidOptions() {
    boolean shouldUpdate = false;
    final List<OptionValueProto> filteredList = new ArrayList<>();
    for (final OptionValueProto optionValueProto : getOptionProtoList()) {
      if(isValid(optionValueProto.getName())) {
        filteredList.add(optionValueProto);
      } else {
        shouldUpdate = true;
        logger.warn("Ignoring deprecated option `{}`", optionValueProto.getName());
      }
    }
    if (shouldUpdate) {
      options.put(OPTIONS_KEY, OptionValueProtoUtils.toOptionValueProtoList(filteredList));
    }
  }

  /**
   * Checks legacy stores and migrates if necessary. Formats should never
   * be mixed, so at most one migration will be performed.
   */
  private void migrateLegacyOptions() {
    if (inMemory) {
      return; // In-memory store does not start with any options
    }
    migrateLegacyJacksonOptions();
    migrateLegacyProtoOptions();
  }

  private void migrateLegacyJacksonOptions() {
    final OptionValueStore legacyStore = new OptionValueStore(
      storeProvider,
      LegacyJacksonOptionStoreCreator.class,
      new JacksonSerializer<>(lpPersistance.getMapper(), OptionValue.class)
    );
    legacyStore.start();

    final List<OptionValueProto> optionList = new ArrayList<>();
    final Iterable<Entry<String, OptionValue>> legacyOptionValues = legacyStore.getAll();
    legacyOptionValues.forEach(
      entry -> {
        if (optionValidatorListing.isValid(entry.getKey())) {
          optionList.add(OptionValueProtoUtils.toOptionValueProto(entry.getValue()));
        }
      }
    );
    if (!optionList.isEmpty()) {
      options.put(OPTIONS_KEY, OptionValueProtoUtils.toOptionValueProtoList(optionList));
      populateCache();
    }
    // Remove after the fact in case migration fails
    legacyOptionValues.forEach(
      entry -> {
        legacyStore.delete(entry.getKey());
      }
    );
  }

  private void migrateLegacyProtoOptions() {
    final LegacyKVStore<String, OptionValueProto> legacyStore = storeProvider.get().getStore(LegacyProtoOptionStoreCreator.class);
    List<OptionValueProto> optionList = new ArrayList<>();
    final Iterable<Entry<String, OptionValueProto>> legacyOptionValues = legacyStore.find();
    legacyOptionValues.forEach(
      entry -> {
        if (optionValidatorListing.isValid(entry.getKey())) {
          optionList.add(entry.getValue());
        }
      }
    );
    if (!optionList.isEmpty()) {
      options.put(OPTIONS_KEY, OptionValueProtoUtils.toOptionValueProtoList(optionList));
      populateCache();
    }
    // Remove after the fact in case migration fails
    legacyOptionValues.forEach(
      entry -> {
        legacyStore.delete(entry.getKey());
      }
    );
  }

  public static class OptionStoreCreator implements LegacyKVStoreCreationFunction<String, OptionValueProtoList> {
    @Override
    public LegacyKVStore<String, OptionValueProtoList> build(LegacyStoreBuildingFactory factory) {
      return factory.<String, OptionValueProtoList>newStore()
        .name(STORE_NAME)
        .keyFormat(Format.ofString())
        .valueFormat(Format.ofProtobuf(OptionValueProtoList.class))
        .build();
    }
  }

  public static class LegacyJacksonOptionStoreCreator implements OptionValueStore.OptionValueStoreCreator {
    @Override
    public LegacyKVStore<String, byte[]> build(LegacyStoreBuildingFactory factory) {
      return factory.<String, byte[]>newStore()
        .name(LEGACY_JACKSON_STORE_NAME)
        .keyFormat(Format.ofString())
        .valueFormat(Format.ofBytes())
        .build();
    }
  }

  public static class LegacyProtoOptionStoreCreator implements LegacyKVStoreCreationFunction<String, OptionValueProto> {
    @Override
    public LegacyKVStore<String, OptionValueProto> build(LegacyStoreBuildingFactory factory) {
      return factory.<String, OptionValueProto>newStore()
        .name(LEGACY_PROTO_STORE_NAME)
        .keyFormat(Format.ofString())
        .valueFormat(Format.ofProtobuf(OptionValueProto.class))
        .build();
    }
  }

  private long getCacheCalls() {
    return cacheCalls;
  }

  private long getKvStoreCalls() {
    return kvStoreCalls;
  }

  public void populateCache() {
    cachedOptionProtoList = getOptionProtoListFromStore();
    notifyListeners();
  }

  @VisibleForTesting
  public void clearCachedOptionProtoList() {
    cachedOptionProtoList = Collections.emptyList();
  }

  public List<OptionValueProto> getOptionProtoListFromStore() {
    final OptionValueProtoList optionValueProtoList = options.get(OPTIONS_KEY);
    kvStoreCalls++;
    return optionValueProtoList == null ? Collections.emptyList() : optionValueProtoList.getOptionsList();
  }

  private List<OptionValueProto> getOptionProtoList() {
    if (broadcaster != null) {
      cacheCalls++;
      return cachedOptionProtoList;
    }
    return getOptionProtoListFromStore();
  }

  private OptionValueProto getOptionProto(String name) {
    for (OptionValueProto optionValueProto : getOptionProtoList()) {
      if (name.toLowerCase(Locale.ROOT).equals(optionValueProto.getName())) {
        return optionValueProto;
      }
    }
    return null;
  }

  @Override
  public boolean isValid(String name){
    return optionValidatorListing.isValid(name);
  }

  @Override
  public boolean isSet(String name){
    return getOptionProto(name) != null;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    return getOptionProtoList().stream()
      .map(OptionValueProtoUtils::toOptionValue)
      .iterator();
  }

  @Override
  public OptionValue getOption(final String name) {
    final OptionValueProto value = getOptionProto(name);
    return value == null ? null : OptionValueProtoUtils.toOptionValue(value);
  }

  @Override
  public boolean setOption(final OptionValue value) {
    checkArgument(value.getType() == OptionType.SYSTEM, "OptionType must be SYSTEM.");
    final String name = value.getName().toLowerCase(Locale.ROOT);
    final OptionValidator validator = optionValidatorListing.getValidator(name);
    validator.validate(value); // validate the option

    final Map<String, OptionValueProto> optionMap = new HashMap<>(); // temp map for convenient lookups
    getOptionProtoList().forEach(optionProto -> optionMap.put(optionProto.getName(), optionProto));

    // no need to set option if value is the same
    if (optionMap.containsKey(name) && optionMap.get(name).equals(OptionValueProtoUtils.toOptionValueProto(value))) {
      return true;
    }

    // Handle setting option to the default value
    if (value.equals(validator.getDefault())) {
      if (optionMap.containsKey(value.getName())) {
        // If option was previously set, remove it
        optionMap.remove(value.getName());
      } else {
        // If option was not set, skip the set completely
        return true;
      }
    }
    optionMap.put(name, OptionValueProtoUtils.toOptionValueProto(value));
    options.put(OPTIONS_KEY, OptionValueProtoUtils.toOptionValueProtoList(optionMap.values()));
    refreshAndNotifySiblings();
    notifyListeners();
    return true;
  }

  @Override
  public boolean deleteOption(final String rawName, OptionType type) {
    checkArgument(type == OptionType.SYSTEM, "OptionType must be SYSTEM.");
    final String name = rawName.toLowerCase(Locale.ROOT);
    optionValidatorListing.getValidator(name); // ensure option exists

    final Pointer<Boolean> needUpdate = new Pointer<>(false);
    final List<OptionValueProto> newOptionValueProtoList = getOptionProtoList().stream()
      .filter(optionValueProto -> {
        if (name.equals(optionValueProto.getName())) {
          needUpdate.value = true;
          return false;
        }
        return true;
      })
      .collect(Collectors.toList());

    if (needUpdate.value) {
      options.put(OPTIONS_KEY, OptionValueProtoUtils.toOptionValueProtoList(newOptionValueProtoList));
      refreshAndNotifySiblings();
    }
    notifyListeners();
    return true;
  }

  @Override
  public boolean deleteAllOptions(OptionType type) {
    checkArgument(type == OptionType.SYSTEM, "OptionType must be SYSTEM.");
    options.put(OPTIONS_KEY, OptionValueProtoList.newBuilder().build());
    refreshAndNotifySiblings();
    notifyListeners();
    return true;
  }

  private void refreshAndNotifySiblings() {
    populateCache();
    if (broadcaster != null) {
      final OptionChangeNotification request = OptionChangeNotification.newBuilder().build();
      try {
        broadcaster.communicateChange(request);
      } catch (Exception e) {
        logger.warn("Unable to communicate system option fetch request with other coordinators.", e);
      }
    }
  }

  private void notifyListeners() {
    listeners.forEach(l -> l.onChange());
  }

  @Override
  public void addOptionChangeListener(OptionChangeListener optionChangeListener) throws UnsupportedOperationException {
    listeners.add(optionChangeListener);
  }

  /**
   * @return all system options that have been set to a non-default value
   */
  @Override
  public OptionList getNonDefaultOptions() {
    final OptionList nonDefaultOptions = new OptionList();
    getOptionProtoList().forEach(
      entry -> nonDefaultOptions.add(OptionValueProtoUtils.toOptionValue(entry))
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

  class FetchSystemOptionTask implements Runnable {

    @Override
    public void run() {
      logger.debug("Background fetch system option from kv store started.");
      populateCache();
      logger.debug("Up to now, there are {} cache calls and {} kv store call for system options", getCacheCalls(), getKvStoreCalls());
    }
  }

}
