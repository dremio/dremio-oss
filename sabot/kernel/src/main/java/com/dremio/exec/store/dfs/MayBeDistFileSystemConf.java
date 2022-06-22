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
 * FileSystemConf for filesystems that may be configured with dist path.
 *
 * @param <C>
 * @param <P>
 */
public abstract class MayBeDistFileSystemConf<C extends FileSystemConf<C, P>, P extends FileSystemPlugin<C>> extends FileSystemConf<C, P> {
  public abstract String getAccessKey();

  public abstract String getSecretKey();

  public abstract String getIamRole();

  public abstract String getExternalId();
}
