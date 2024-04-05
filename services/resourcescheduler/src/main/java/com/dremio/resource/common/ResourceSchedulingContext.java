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
package com.dremio.resource.common;

import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.options.OptionManager;
import java.util.Collection;

/** To provide resource scheduling context interface */
public interface ResourceSchedulingContext {

  CoordExecRPC.QueryContextInformation getQueryContextInfo();

  UserBitShared.QueryId getQueryId();

  String getQueryUserName();

  CoordinationProtos.NodeEndpoint getCurrentEndpoint();

  Collection<CoordinationProtos.NodeEndpoint> getActiveEndpoints();

  OptionManager getOptions();
}
