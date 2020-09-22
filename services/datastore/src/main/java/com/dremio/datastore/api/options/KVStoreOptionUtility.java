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

package com.dremio.datastore.api.options;

import java.util.Arrays;
import java.util.Optional;

import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.indexed.IndexPutOption;

/**
 * Utility class to perform validation of {@link KVStore.KVStoreOption} values.
 */
public class KVStoreOptionUtility {

  /**
   * Checks the array of PutOption value to ensure that conflicting CREATE and VERSION options do
   * not exist at the same time.
   *
   * @param options Options array to check.
   */
  public static void validateOptions(KVStore.KVStoreOption... options) {
    if (null == options || options.length <= 1) {
      return;
    }

    boolean foundVersion = false;
    boolean foundCreate = false;

    for (KVStore.KVStoreOption option : options) {
      if (option == KVStore.PutOption.CREATE) {
        if (foundCreate) {
          throw new IllegalArgumentException("Multiple CREATE PutOptions supplied.");
        } else {
          foundCreate = true;
        }
      } else if (option instanceof VersionOption) {
        if (foundVersion) {
          throw new IllegalArgumentException("Multiple Version PutOptions supplied.");
        } else {
          foundVersion = true;
        }
      }
    }
    if (foundCreate && foundVersion) {
      throw new IllegalArgumentException("Conflicting PutOptions supplied.");
    }
  }

  /**
   * Helper function to extract PutOption objects which are of the Type CREATE or VERSION from a
   * list of KVStoreOption objects.
   *
   * @param options The array of options to search.
   * @return The found PutOption of type CREATE or VERSION or null if neither are found.
   */
  public static Optional<KVStore.PutOption> getCreateOrVersionOption(KVStore.KVStoreOption... options) {
    validateOptions(options);

    if (null == options || options.length == 0) {
      return Optional.empty();
    }

    for (KVStore.KVStoreOption option : options) {
      if (option == KVStore.PutOption.CREATE) {
        return Optional.of((KVStore.PutOption) option);
      } else if (option instanceof VersionOption) {
        return Optional.of((VersionOption) option);
      }
    }

    return Optional.empty();
  }

  /**
   * Helper function to validate that IndexPutOption does not exist in the array of KVStoreOption
   * objects.
   *
   * @param options The array of options to search.
   * @throws IllegalArgumentException if an instance of IndexPutOption is found.
   */
  public static void checkIndexPutOptionIsNotUsed(KVStore.KVStoreOption... options) {
    if (null == options || options.length == 0) {
      return;
    }

    for (KVStore.KVStoreOption option : options) {
      if (option instanceof IndexPutOption) {
        throw new IllegalArgumentException("IndexPutOption not supported.");
      }
    }
  }

  /**
   * Remove instance of IndexPutOption from the array.
   *
   * @param options Incoming PutOption list that may contain an IndexPutOption.
   * @return An array with all non-IndexPutOption elements.
   */
  public static KVStore.PutOption[] removeIndexPutOption(KVStore.PutOption... options) {
    if (null == options) {
      return null;
    }

    return Arrays
      .stream(options)
      .filter(option -> !(option instanceof IndexPutOption))
      .toArray(KVStore.PutOption[]::new);
  }
}
