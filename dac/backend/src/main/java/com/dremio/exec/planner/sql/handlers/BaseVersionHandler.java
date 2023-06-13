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
package com.dremio.exec.planner.sql.handlers;

import static java.util.Objects.requireNonNull;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.sql.handlers.direct.SqlDirectHandler;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.options.OptionResolver;
import com.dremio.service.namespace.NamespaceNotFoundException;

/**
 * Base class for show handlers, create folder handler.
 */
public abstract class BaseVersionHandler<T> implements SqlDirectHandler<T> {
  private final Catalog catalog;
  private OptionResolver optionResolver;

  // for CREATE FOLDERS
  protected BaseVersionHandler(Catalog catalog) {
    this.catalog = requireNonNull(catalog);
  }
  protected BaseVersionHandler(Catalog catalog, OptionResolver optionResolver) {
    this(catalog);
    this.optionResolver = requireNonNull(optionResolver);
  }

  protected void checkFeatureEnabled(String message) {
    if (!optionResolver.getOption(ExecConstants.ENABLE_USE_VERSION_SYNTAX)) {
      throw UserException.unsupportedError().message(message).buildSilently();
    }
  }

  protected VersionedPlugin getVersionedPlugin(String sourceName) {
    final StoragePlugin storagePlugin;
    try {
      storagePlugin = catalog.getSource(sourceName);
    } catch (UserException e) {
      if (e.getErrorType() != UserBitShared.DremioPBError.ErrorType.VALIDATION) {
        // Some unknown error, rethrow
        throw e;
      }

      if(e.getCause() instanceof NamespaceNotFoundException){
        throw UserException.validationError(e)
          .message("Source %s does not exist.", sourceName)
          .buildSilently();
      } else {
        // Source was not found (probably wrong type, like home)
        throw UserException.unsupportedError(e)
          .message("Source %s does not support versioning.", sourceName)
          .buildSilently();
      }
    }
    if (!(storagePlugin instanceof VersionedPlugin)) {
      throw UserException.unsupportedError()
          .message("Source %s does not support versioning.", sourceName)
          .buildSilently();
    }

    return (VersionedPlugin) storagePlugin;
  }


}
