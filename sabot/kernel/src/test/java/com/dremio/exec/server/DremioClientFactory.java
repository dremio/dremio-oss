/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.arrow.memory.OutOfMemoryException;
import org.glassfish.hk2.api.Factory;

import com.dremio.exec.client.DremioClient;

public class DremioClientFactory implements Factory<DremioClient> {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioClientFactory.class);

  @Override
  public void dispose(DremioClient arg0) {
  }

  @Override
  public DremioClient provide() {
    try {
      return new DremioClient();
    } catch(OutOfMemoryException e) {
      throw new RuntimeException(e);
    }
  }
}
