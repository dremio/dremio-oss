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

import { useReducer } from "react";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

import AboutModalTabs from "./AboutModalTabs";
import BuildInformation from "./BuildInformation";
import ClusterUsageData from "./ClusterUsageData";
import { AboutModalReducer, initialState } from "./aboutModalReducer";

import { formDescription } from "@app/uiTheme/radium/typography";
import * as classes from "./AboutModal.module.less";

const { t } = getIntlContext();

const isBeta = process.env.DREMIO_BETA === "true";

function AboutModalInfoPane() {
  const [state, dispatch] = useReducer(AboutModalReducer, initialState);

  return (
    <div className={classes["info-pane"]}>
      <div className={classes["info-pane-contents"]}>
        <div className={classes["about-title"]}>
          {isBeta ? t("App.DremioBeta") : t("App.Dremio")}
        </div>
        <AboutModalTabs state={state} dispatch={dispatch} />
        {state.activeTab === "build" ? (
          <BuildInformation state={state} dispatch={dispatch} />
        ) : (
          <ClusterUsageData state={state} dispatch={dispatch} />
        )}
      </div>
      <div style={formDescription}>{t("App.Copyright")}</div>
      <div style={formDescription}>{isBeta && t("App.ProductionWarning")}</div>
    </div>
  );
}

export default AboutModalInfoPane;
