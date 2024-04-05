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
package com.dremio.exec.store.iceberg.nessie;

import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import javax.annotation.Nullable;

public interface IcebergCommitOriginAwareTableOperations {

  /** updates the origin to the new value if it currently has the old value */
  void tryNarrowCommitOrigin(
      @Nullable IcebergCommitOrigin oldOrigin, IcebergCommitOrigin newOrigin);
}
