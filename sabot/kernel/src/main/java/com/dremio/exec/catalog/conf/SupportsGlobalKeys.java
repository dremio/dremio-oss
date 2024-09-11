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
package com.dremio.exec.catalog.conf;

import java.util.List;

/**
 * This is an optional interface. When extended by an implementation, this is used to provide
 * project keys for the SourcePlugin
 */
public interface SupportsGlobalKeys {

  void setGlobalKeys(List<Property> globalKeys);

  List<Property> getGlobalKeys();

  boolean isUsingGlobalKeys();
}
