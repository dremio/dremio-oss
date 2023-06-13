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
package com.dremio.service.nessie;

import javax.inject.Provider;

import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;

import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.PositiveLongValidator;

/**
 * Nessie datastore database adapter config
 */
@Options
public class NessieDatabaseAdapterConfig implements NonTransactionalDatabaseAdapterConfig {

  // Note: config variable definitions need to be in public static fields for
  // Dremio runtime to be able to introspect them.

  public static final PositiveLongValidator COMMIT_TIMEOUT_MS = new PositiveLongValidator(
      "nessie.kvversionstore.commit_timeout_ms", Integer.MAX_VALUE, 600_000L);

  public static final PositiveLongValidator COMMIT_RETRIES = new PositiveLongValidator(
      "nessie.kvversionstore.commit_retries", Integer.MAX_VALUE, Integer.MAX_VALUE); // no retry limit by default

  public static final PositiveLongValidator PARENTS_PER_COMMIT = new PositiveLongValidator(
    "nessie.kvversionstore.parents_per_commit", Integer.MAX_VALUE, DEFAULT_PARENTS_PER_COMMIT);

  public static final PositiveLongValidator PARENTS_PER_REFLOG_ENTRY = new PositiveLongValidator(
    "nessie.kvversionstore.parents_per_reflog_entry", Integer.MAX_VALUE, DEFAULT_PARENTS_PER_REFLOG_ENTRY);

  public static final PositiveLongValidator KEY_LIST_DISTANCE = new PositiveLongValidator(
      "nessie.kvversionstore.key_list_distance", Integer.MAX_VALUE, DEFAULT_KEY_LIST_DISTANCE);

  public static final PositiveLongValidator MAX_KEY_LIST_SIZE = new PositiveLongValidator(
      "nessie.kvversionstore.max_key_list_size", Integer.MAX_VALUE, DEFAULT_MAX_ENTITY_SIZE);

  // Note: default retry timeouts are set based on DCS experience
  public static final PositiveLongValidator RETRY_INITIAL_SLEEP_MILLIS_LOWER = new PositiveLongValidator(
      "nessie.kvversionstore.retry_initial_sleep_millis_lower", Integer.MAX_VALUE, 2);

  // Note: default retry timeouts are set based on DCS experience
  public static final PositiveLongValidator RETRY_INITIAL_SLEEP_MILLIS_UPPER = new PositiveLongValidator(
      "nessie.kvversionstore.retry_initial_sleep_millis_upper", Integer.MAX_VALUE, 100);

  // Note: default retry timeouts are set based on DCS experience
  public static final PositiveLongValidator RETRY_MAX_SLEEP_MILLIS = new PositiveLongValidator(
      "nessie.kvversionstore.retry_max_sleep_millis", Integer.MAX_VALUE, 3200);

  private final Provider<OptionManager> optionManager;

  public NessieDatabaseAdapterConfig(Provider<OptionManager> optionManager) {
    this.optionManager = optionManager;
  }

  @Override
  public boolean validateNamespaces() {
    return false;
  }

  @Override
  public int getCommitRetries() {
    return (int) optionManager.get().getOption(COMMIT_RETRIES);
  }

  @Override
  public long getCommitTimeout() {
    return optionManager.get().getOption(COMMIT_TIMEOUT_MS);
  }

  @Override
  public int getParentsPerCommit() {
    return (int) optionManager.get().getOption(PARENTS_PER_COMMIT);
  }

  @Override
  public int getParentsPerRefLogEntry() {
    return (int) optionManager.get().getOption(PARENTS_PER_REFLOG_ENTRY);
  }

  @Override
  public int getKeyListDistance() {
    return (int) optionManager.get().getOption(KEY_LIST_DISTANCE);
  }

  @Override
  public int getMaxKeyListSize() {
    return (int) optionManager.get().getOption(MAX_KEY_LIST_SIZE);
  }

  @Override
  public long getRetryInitialSleepMillisLower() {
    return optionManager.get().getOption(RETRY_INITIAL_SLEEP_MILLIS_LOWER);
  }

  @Override
  public long getRetryInitialSleepMillisUpper() {
    return optionManager.get().getOption(RETRY_INITIAL_SLEEP_MILLIS_UPPER);
  }

  @Override
  public long getRetryMaxSleepMillis() {
    return optionManager.get().getOption(RETRY_MAX_SLEEP_MILLIS);
  }
}
