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

import { useEffect } from "react";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import { Spinner } from "dremio-ui-lib/components";

import { AboutModalState, ActionTypes } from "./aboutModalReducer";
import { useBuildInfo } from "@app/exports/providers/AboutModalProviders";
import { isSmartFetchLoading } from "@app/utils/isSmartFetchLoading";
import { getEdition } from "dyn-load/utils/versionUtils";
import timeUtils from "@app/utils/timeUtils";

import * as classes from "./AboutModal.module.less";

type BuildInformationProps = {
  state: AboutModalState;
  dispatch: React.Dispatch<ActionTypes>;
};

const { t } = getIntlContext();

function BuildInformation({ state, dispatch }: BuildInformationProps) {
  const { value: buildInfo, status: buildInfoStatus } = useBuildInfo(
    !state.alreadyFetchedBuildInfo
  );

  useEffect(
    () =>
      dispatch({
        type: "SET_ALREADY_FETCHED_BUILD_INFO",
        alreadyFetchedBuildInfo: true,
      }),
    [dispatch]
  );

  const buildTime = timeUtils.formatTime(
    buildInfo?.buildTime,
    undefined,
    undefined,
    timeUtils.formats.ISO
  );

  const commitTime = timeUtils.formatTime(
    buildInfo?.commit.time,
    undefined,
    undefined,
    timeUtils.formats.ISO
  );

  return isSmartFetchLoading(buildInfoStatus) ? (
    <Spinner className={classes["about-spinner"]} />
  ) : (
    <dl className="normalFontSize">
      <dt className={classes["version-description-term"]}>{t("App.Build")}</dt>
      <dd>{buildInfo?.version ?? "-"}</dd>
      <dt className={classes["version-description-term"]}>
        {t("App.Edition")}
      </dt>
      <dd>{getEdition()}</dd>
      <dt className={classes["version-description-term"]}>
        {t("App.BuildTime")}
      </dt>
      <dd>{buildTime}</dd>
      <dt className={classes["version-description-term"]}>
        {t("App.ChangeHash")}
      </dt>
      <dd>{buildInfo?.commit.hash ?? "-"}</dd>
      <dt className={classes["version-description-term"]}>
        {t("App.ChangeTime")}
      </dt>
      <dd>{commitTime}</dd>
    </dl>
  );
}

export default BuildInformation;
