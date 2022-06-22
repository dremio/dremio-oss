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
package com.dremio.exec.planner.sql.handlers.direct;

import static com.dremio.exec.ExecConstants.ENABLE_USE_VERSION_SYNTAX;

import java.util.List;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.options.OptionResolver;
import com.dremio.service.namespace.NamespaceKey;

class ShowHandlerUtil {
  // prevent instantiation
  private ShowHandlerUtil() {
  }

  static VersionedPlugin getVersionedPlugin(String sourceName, Catalog catalog) {
    final StoragePlugin storagePlugin;
    try {
      storagePlugin = catalog.getSource(sourceName);
    } catch (UserException e) {
      if (e.getErrorType() != UserBitShared.DremioPBError.ErrorType.VALIDATION) {
        // Some unknown error, rethrow
        throw e;
      }
      // Source was not found (probably wrong type, like home)
      throw UserException.unsupportedError()
        .message("Source %s does not support versioning.", sourceName).buildSilently();
    }

    if (!(storagePlugin instanceof VersionedPlugin)) {
      throw UserException.unsupportedError()
        .message("Source %s does not support versioning.", sourceName).buildSilently();
    }

    return (VersionedPlugin) storagePlugin;
  }

  static void validate(NamespaceKey sourcePath, Catalog catalog) {
    if (sourcePath == null) {
      // If the default schema is a root schema, throw an error to select a default schema
      throw UserException.validationError()
        .message("No default schema selected. Select a schema using 'USE schema' command.")
        .buildSilently();
    }

    if (!catalog.containerExists(new NamespaceKey(sourcePath.getRoot()))) {
      throw UserException.validationError()
        .message("Source %s does not exist.", sourcePath.getRoot())
        .buildSilently();
    }
  }

  static void checkVersionedFeatureEnabled(OptionResolver optionResolver, String message) {
    if (!optionResolver.getOption(ENABLE_USE_VERSION_SYNTAX)) {
      throw UserException.unsupportedError().message(message).buildSilently();
    }
  }

  static String concatSourceNameAndNamespace(String sourceName, List<String> namespace) {
    String sourceNameAndNamespace = sourceName;
    if (!namespace.isEmpty()) {
      sourceNameAndNamespace = String.join(".", sourceName, String.join(".", namespace));
    }
    return sourceNameAndNamespace;
  }
}
