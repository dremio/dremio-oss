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
package com.dremio.sabot.exec.context;

import com.dremio.exec.proto.CoordExecRPC.QueryContextInformation;
import com.dremio.exec.proto.UserBitShared.UserCredentials;

/**
 * Factory to create contextual information during execution.
 */
public class ContextInformationFactory {

  public ContextInformation newContextFactory(
      final UserCredentials userCredentials,
      QueryContextInformation queryContextInfo) {
    return new ContextInformationImpl(userCredentials, queryContextInfo);
  }

}
