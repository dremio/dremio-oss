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
package com.dremio.dac.server.admin.profile;

import com.dremio.exec.proto.UserBitShared;

public class SourceVersionWrapper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SourceVersionWrapper.class);

  private String source;
  private String versionContext;
  private UserBitShared.SourceVersionSetting.Usage  usage;

  public SourceVersionWrapper(String source, String versionContext, UserBitShared.SourceVersionSetting.Usage usage) {
    this.source = source;
    this.versionContext = versionContext;
    this.usage = usage;
  }

  UserBitShared.SourceVersionSetting.Usage getUsage() {
    return usage;
  }
  public static final String[] SOURCE_VERSION_COLUMNS = {"Source Name", "Version Context"};

  public void addSummary(TableBuilder tb) {
    tb.appendCell(source);
    tb.appendCell(versionContext);
  }
}
