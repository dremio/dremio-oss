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
package com.dremio.sabot.exec.context;

import com.dremio.exec.context.AdditionalContext;
import com.dremio.exec.proto.UserBitShared.QueryId;

/**
 * Provides query context information (such as query start time, query user, default schema etc.) for UDFs.
 */
public interface ContextInformation {

  /**
   * @return userName of the user who issued the current query.
   */
  String getQueryUser();

  /**
   * @return Get the current default schema in user session at the time of this particular query submission.
   */
  String getCurrentDefaultSchema();

  /**
   * @return Query start time in milliseconds
   */
  long getQueryStartTime();

  /**
   * @return Time zone.
   */
  int getRootFragmentTimeZone();

  /**
   * @return Last Query ID
   */
  QueryId getLastQueryId();

  void registerAdditionalInfo(AdditionalContext object);

  <T extends AdditionalContext> T getAdditionalInfo(Class<T> clazz);
}
