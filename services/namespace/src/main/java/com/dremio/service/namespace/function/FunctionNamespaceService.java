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
package com.dremio.service.namespace.function;

import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import java.util.List;

/** Namespace operations for Functions. */
public interface FunctionNamespaceService {
  void addOrUpdateFunction(
      NamespaceKey functionPath, FunctionConfig functionConfig, NamespaceAttribute... attributes)
      throws NamespaceException;

  FunctionConfig getFunction(NamespaceKey functionPath) throws NamespaceException;

  List<FunctionConfig> getFunctions();

  void deleteFunction(NamespaceKey functionPath) throws NamespaceException;
}
