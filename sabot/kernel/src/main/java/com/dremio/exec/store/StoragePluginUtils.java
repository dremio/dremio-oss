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
package com.dremio.exec.store;

import com.dremio.common.exceptions.UserException;

/**
 * Utility class for Storage plugins.
 */
public final class StoragePluginUtils {
  private StoragePluginUtils() {}

  /**
   * Generates an error message with the following format:
   * [%s] %s, where the first parameter indicates the source name and the second refers to the error.
   * @param storagePluginName Name of the storage plugin where the error is generated.
   * @param errorMessage Error message received from the storage plugin.
   * @return Generated error message.
   */
  public static String generateSourceErrorMessage(final String storagePluginName, String errorMessage) {
    return String.format("Source '%s' returned error '%s'", storagePluginName, errorMessage);
  }

  /**
   * Generates an error message given a source name, an error format string and format arguments.
   * @param storagePluginName Name of the storage plugin where the error is generated.
   * @param errorMessage Format string for the error message received from the storage plugin.
   * @param args Arguments to format the error message with.
   * @return Generated error message.
   */
  public static String generateSourceErrorMessage(final String storagePluginName, String errorMessage, Object... args) {
    final String formattedMessage = String.format(errorMessage, args);
    return generateSourceErrorMessage(storagePluginName, formattedMessage);
  }

  /**
   * Given a {@code UserException.Builder} instance, adds a message and the source name as context to the instance.
   * @param builder The UserException.Builder instance
   * @param errorMessage The format string to be used for the error message
   * @param sourceName The name of the source to be added to the context of the UserException.Builder instance.
   * @param args Arguments referenced by the format specifiers in the format string.
   * @return The modified UserException.Builder instance.
   */
  public static UserException.Builder message(UserException.Builder builder, String sourceName, String errorMessage, Object... args) {
    // Constructs a format string which contains the sourceName and the errorMessage,
    // which is then formatted with arguments provided as parameters.
    return builder.message(generateSourceErrorMessage(sourceName, errorMessage), args)
      .addContext("plugin", sourceName);
  }
}
