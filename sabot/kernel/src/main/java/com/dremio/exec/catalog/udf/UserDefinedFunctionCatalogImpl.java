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
package com.dremio.exec.catalog.udf;

import static com.dremio.exec.catalog.CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED;
import static com.dremio.exec.store.sys.udf.UserDefinedFunctionSerde.fromProto;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.SourceCatalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.exec.store.sys.udf.UserDefinedFunctionSerde;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

public class UserDefinedFunctionCatalogImpl implements UserDefinedFunctionCatalog {
  private final QueryContext queryContext;
  private final OptionManager optionManager;
  private final NamespaceService userNamespaceService;
  private final SourceCatalog sourceCatalog;

  public UserDefinedFunctionCatalogImpl(
      QueryContext queryContext,
      OptionManager optionManager,
      NamespaceService userNamespaceService,
      SourceCatalog sourceCatalog) {
    this.queryContext = queryContext;
    this.optionManager = optionManager;
    this.userNamespaceService = userNamespaceService;
    this.sourceCatalog = sourceCatalog;
  }

  @Override
  public void createFunction(CatalogEntityKey key, UserDefinedFunction userDefinedFunction) {
    createOrUpdateFunction(key, userDefinedFunction, false);
  }

  @Override
  public void updateFunction(CatalogEntityKey key, UserDefinedFunction userDefinedFunction) {
    createOrUpdateFunction(key, userDefinedFunction, true);
  }

  private void createOrUpdateFunction(
      CatalogEntityKey key, UserDefinedFunction userDefinedFunction, boolean isUpdate) {
    // TODO - Block any path that is not in a space or home space.
    try {
      if (CatalogUtil.requestedPluginSupportsVersionedTables(key.toNamespaceKey(), sourceCatalog)) {
        MutablePlugin mutablePlugin = null;
        if (optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)) {
          StoragePlugin plugin = sourceCatalog.getSource(key.getRootEntity());
          if (plugin != null && plugin.isWrapperFor(MutablePlugin.class)) {
            mutablePlugin = plugin.unwrap(MutablePlugin.class);
          }
        }
        if (mutablePlugin != null) {
          mutablePlugin.createFunction(
              getKeyWithVersionContext(key), queryContext.getSchemaConfig(), userDefinedFunction);
        } else {
          throw UserException.unsupportedError()
              .message(
                  "You cannot store a user-defined function in source '%s'.", key.getRootEntity())
              .buildSilently();
        }
      } else {
        ensureNoVersionContext(key);

        DremioSqlOperatorTable systemFunctionTable = DremioSqlOperatorTable.instance();
        String udfName = key.toNamespaceKey().getName();
        boolean nameConflictWithSystemFunction =
            key.toNamespaceKey().getPathComponents().size() == 1
                && systemFunctionTable.getOperatorList().stream()
                    .anyMatch(operator -> operator.getName().equalsIgnoreCase(udfName));
        if (nameConflictWithSystemFunction) {
          throw UserException.validationError()
              .message(
                  "There is a system function with name %s", key.getLeaf().toUpperCase(Locale.ROOT))
              .buildSilently();
        }

        FunctionConfig newFunctionConfig = UserDefinedFunctionSerde.toProto(userDefinedFunction);
        if (isUpdate) {
          try {
            FunctionConfig oldFunctionConfig =
                userNamespaceService.getFunction(key.toNamespaceKey());
            newFunctionConfig.setTag(oldFunctionConfig.getTag()).setId(oldFunctionConfig.getId());
          } catch (NamespaceException ignore) {
            // It's fine that the tag is not copied over if the existing function not found.
          }
        }

        userNamespaceService.addOrUpdateFunction(key.toNamespaceKey(), newFunctionConfig);
      }
    } catch (UserException ue) {
      throw ue;
    } catch (Exception exception) {
      // TODO
      throw new RuntimeException(exception);
    }
  }

  @Override
  public void dropFunction(CatalogEntityKey key) {
    try {
      if (CatalogUtil.requestedPluginSupportsVersionedTables(key.toNamespaceKey(), sourceCatalog)) {
        MutablePlugin mutablePlugin = null;
        if (optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)) {
          StoragePlugin plugin = sourceCatalog.getSource(key.getRootEntity());
          if (plugin != null && plugin.isWrapperFor(MutablePlugin.class)) {
            mutablePlugin = plugin.unwrap(MutablePlugin.class);
          }
        }
        if (mutablePlugin != null) {
          mutablePlugin.dropFunction(getKeyWithVersionContext(key), queryContext.getSchemaConfig());
        } else {
          throw UserException.unsupportedError()
              .message("Drop function in source '%s' not supported.", key.getRootEntity())
              .buildSilently();
        }
      } else {
        ensureNoVersionContext(key);
        userNamespaceService.deleteFunction(key.toNamespaceKey());
      }
    } catch (Exception exception) {
      // TODO
      throw new RuntimeException(exception);
    }
  }

  @Override
  public UserDefinedFunction getFunction(CatalogEntityKey key) {
    try {
      VersionedPlugin versionedPlugin;
      if (CatalogUtil.requestedPluginSupportsVersionedTables(key.toNamespaceKey(), sourceCatalog)
          && optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)) {
        StoragePlugin plugin = sourceCatalog.getSource(key.getRootEntity());
        if (plugin != null && plugin.isWrapperFor(VersionedPlugin.class)) {
          versionedPlugin = plugin.unwrap(VersionedPlugin.class);
          Optional<FunctionConfig> function =
              versionedPlugin.getFunction(getKeyWithVersionContext(key));
          if (function.isPresent()) {
            return fromProto(function.get());
          }
          throw UserException.resourceError()
              .message("Cannot find function with name %s", key)
              .buildSilently();
        }
      }
      ensureNoVersionContext(key);
      return fromProto(userNamespaceService.getFunction(key.toNamespaceKey()));
    } catch (NamespaceNotFoundException e) {
      return null;
    } catch (NamespaceException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<UserDefinedFunction> getAllFunctions() {
    return userNamespaceService.getFunctions().stream()
        .map(UserDefinedFunctionSerde::fromProto)
        .collect(Collectors.toList());
  }

  private CatalogEntityKey getKeyWithVersionContext(CatalogEntityKey key) {
    if (!key.hasTableVersionContext()) {
      return CatalogEntityKey.newBuilder()
          .keyComponents(key.getKeyComponents())
          .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
          .build();
    }

    return key;
  }

  private void ensureNoVersionContext(CatalogEntityKey key) {
    if (key.hasTableVersionContext()) {
      throw UserException.validationError()
          .message(
              "Version context '%s' not supported in '%s'",
              key.getTableVersionContext(), key.getRootEntity())
          .buildSilently();
    }
  }
}
