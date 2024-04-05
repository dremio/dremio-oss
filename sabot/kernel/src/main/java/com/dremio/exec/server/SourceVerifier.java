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
package com.dremio.exec.server;

import com.dremio.exec.server.options.SystemOptionManager;

/** Interface which verifies a source type is supported or not. */
public interface SourceVerifier {
  boolean isSourceSupported(String sourceName, SystemOptionManager systemOptionManager);

  /** NO_OP implementation */
  SourceVerifier NO_OP =
      (sourceName, systemOptionManager) -> {
        return true;
      };
}
