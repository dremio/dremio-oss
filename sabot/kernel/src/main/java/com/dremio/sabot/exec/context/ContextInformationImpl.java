/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.dremio.exec.context.AdditionalContext;
import com.dremio.exec.proto.CoordExecRPC.QueryContextInformation;
import com.dremio.exec.proto.UserBitShared.UserCredentials;

/**
 * Default implementation of ContextInformation.
 */
public class ContextInformationImpl implements ContextInformation {
  private final String queryUser;
  private final String currentDefaultSchema;
  private final long queryStartTime;
  private final int rootFragmentTimeZone;
  private final Map<Class<? extends AdditionalContext>, AdditionalContext> additionalInfo = new ConcurrentHashMap<>(1);

  public ContextInformationImpl(final UserCredentials userCredentials, final QueryContextInformation queryContextInfo) {
    this.queryUser = userCredentials.getUserName();
    this.currentDefaultSchema = queryContextInfo.getDefaultSchemaName();
    this.queryStartTime = queryContextInfo.getQueryStartTime();
    this.rootFragmentTimeZone = queryContextInfo.getTimeZone();
  }

  @Override
  public String getQueryUser() {
    return queryUser;
  }

  @Override
  public String getCurrentDefaultSchema() {
    return currentDefaultSchema;
  }

  @Override
  public long getQueryStartTime() {
    return queryStartTime;
  }

  @Override
  public int getRootFragmentTimeZone() {
    return rootFragmentTimeZone;
  }

  @Override
  public void registerAdditionalInfo(AdditionalContext object) {
    // this event is rare and mostly once in the ContextInformation lifetime
    // should be once per AdditionalContext type
    AdditionalContext prevValue = additionalInfo.putIfAbsent(object.getClass(), object);
    if (prevValue != null) {
      throw new RuntimeException("Trying to set AdditionalContext of type: " + object.getClass() + " multiple times");
    }
  }

  @Override
  public <T extends AdditionalContext> T getAdditionalInfo(Class<T> claz) {
    return (T) additionalInfo.get(claz);
  }
}
