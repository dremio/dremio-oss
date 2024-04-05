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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.SourceCatalog;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.exec.store.sys.udf.UserDefinedFunctionSerde;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import java.util.stream.Collectors;

public class UserDefinedFunctionCatalogImpl implements UserDefinedFunctionCatalog {
  private final OptionManager optionManager;
  private final NamespaceService userNamespaceService;
  private final SourceCatalog sourceCatalog;

  public UserDefinedFunctionCatalogImpl(
      OptionManager optionManager,
      NamespaceService userNamespaceService,
      SourceCatalog sourceCatalog) {
    this.optionManager = optionManager;
    this.userNamespaceService = userNamespaceService;
    this.sourceCatalog = sourceCatalog;
  }

  @Override
  public void createFunction(NamespaceKey key, UserDefinedFunction userDefinedFunction) {
    // TODO - Block any path that is not in a space or home space.
    if (!optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)
        && CatalogUtil.requestedPluginSupportsVersionedTables(key, sourceCatalog)) {
      throw UserException.unsupportedError()
          .message("You cannot store a user-defined function in source '%s'.", key.getRoot())
          .buildSilently();
    }

    try {
      userNamespaceService.addOrUpdateFunction(
          key, UserDefinedFunctionSerde.toProto(userDefinedFunction));
    } catch (NamespaceException namespaceException) {
      // TODO
      throw new RuntimeException(namespaceException);
    }
  }

  @Override
  public void updateFunction(NamespaceKey key, UserDefinedFunction userDefinedFunction) {
    try {
      FunctionConfig oldFunctionConfig = userNamespaceService.getFunction(key);
      userNamespaceService.addOrUpdateFunction(
          key,
          UserDefinedFunctionSerde.toProto(userDefinedFunction)
              .setTag(oldFunctionConfig.getTag())
              .setId(oldFunctionConfig.getId()));
    } catch (NamespaceException namespaceException) {
      // TODO
      throw new RuntimeException(namespaceException);
    }
  }

  @Override
  public void dropFunction(NamespaceKey key) {
    try {
      userNamespaceService.deleteFunction(key);
    } catch (NamespaceException namespaceException) {
      // TODO
      throw new RuntimeException(namespaceException);
    }
  }

  @Override
  public UserDefinedFunction getFunction(NamespaceKey key) {
    try {
      return fromProto(userNamespaceService.getFunction(key));
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
}
