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
package com.dremio.dac.server.models;

import com.dremio.dac.explore.bi.QlikResource;
import com.dremio.dac.options.PowerBIResourceOptions;
import com.dremio.dac.options.TableauResourceOptions;
import com.dremio.options.OptionManager;
import com.fasterxml.jackson.annotation.JsonCreator;

/** AnalyzeTools */
public final class AnalyzeTools {
  private final BITool tableau;
  private final BITool powerbi;
  private final BITool qlik;

  @JsonCreator
  private AnalyzeTools(BITool tableau, BITool powerbi, BITool qlik) {
    this.tableau = tableau;
    this.powerbi = powerbi;
    this.qlik = qlik;
  }

  public BITool getTableau() {
    return tableau;
  }

  public BITool getPowerbi() {
    return powerbi;
  }

  public BITool getQlik() {
    return qlik;
  }

  public static AnalyzeTools from(OptionManager optionManager) {
    return new AnalyzeTools(
        new BITool(optionManager.getOption(TableauResourceOptions.CLIENT_TOOLS_TABLEAU)),
        new BITool(optionManager.getOption(PowerBIResourceOptions.CLIENT_TOOLS_POWERBI)),
        new BITool(optionManager.getOption(QlikResource.CLIENT_TOOLS_QLIK)));
  }

  /** BITool */
  private static final class BITool {
    private final boolean enabled;

    @JsonCreator
    private BITool(boolean enabled) {
      this.enabled = enabled;
    }

    public boolean isEnabled() {
      return enabled;
    }
  }
}
