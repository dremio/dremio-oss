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
package com.dremio.exec.store.dfs;

/**
 * An abstraction providing a read-only property bag interface on top of some concrete filesystem configuration.
 * This provides a way for exposing filesystem configuration without introducing direct dependencies on Hadoop
 * Configuration objects.
 */
public interface FileSystemConfigurationAdapter {
  String get(String name);
  String get(String name, String defaultValue);
}
