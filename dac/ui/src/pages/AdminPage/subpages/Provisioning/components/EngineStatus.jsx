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
import PropTypes from "prop-types";
import Immutable from "immutable";
import {
  CLUSTER_STATE,
  CLUSTER_STATE_LEGEND_ICON,
  CLUSTER_STATE_ICON,
} from "@inject/constants/provisioningPage/provisioningConstants";
import clsx from "clsx";
import { Tooltip } from "dremio-ui-lib";
import LottieImages from "@app/components/LottieImages";
import { getIconPath } from "@app/utils/getIconPath";
import * as classes from "./EngineStatus.module.less";
import StoppingEngineLottie from "@app/art/StoppingEngine.lottie";
import StartingEngineLottie from "@app/art/StartingEngineNew.lottie";
import { Spinner } from "dremio-ui-lib/components";

const unknownStateIcon = CLUSTER_STATE_ICON[CLUSTER_STATE.unknown];

export function EngineStatusIcon(props) {
  const { status, isInProgress, type, count } = props;
  let icon =
    type === "legend"
      ? CLUSTER_STATE_LEGEND_ICON[status]
      : CLUSTER_STATE_ICON[status];
  if (!icon) {
    icon = {
      ...unknownStateIcon,
      text: `${unknownStateIcon.text}, currentStatus: ${status}`,
    };
  }
  if (status === "STARTING" && count > 0) {
    icon.src = StartingEngineLottie;
  } else if (status === "STARTING" && count === 0) {
    icon.src = "starting-static";
  }
  if (status === "STOPPING" && count > 0) {
    icon.src = StoppingEngineLottie;
  } else if (status === "STOPPING" && count === 0) {
    icon.src = "stopping-static";
  }
  if (icon.src.endsWith(".lottie")) {
    return (
      <Tooltip title={icon.text}>
        <div className={icon.className}>
          <LottieImages
            src={icon.src}
            imageWidth={24}
            imageHeight={24}
            alt={icon.text}
            title
            style={{ marginRight: "5px" }}
          />
        </div>
      </Tooltip>
    );
  } else if (status === "STARTING" || status === "STOPPING") {
    return (
      <Tooltip title={icon.text}>
        {isInProgress ? (
          <Spinner />
        ) : (
          <img
            src={getIconPath(`engine-state/${icon.src}`)}
            alt={icon.text}
            className={classes["engineStatus__svgIcon"]}
          />
        )}
      </Tooltip>
    );
  }
  return (
    <Tooltip title={icon.text}>
      <dremio-icon
        name={`engine-state/${icon.src}`}
        alt={icon.text}
        class={clsx(
          classes["engineStatus__icon"],
          classes["engineStatus__svgIcon"],
        )}
      />
    </Tooltip>
  );
}
EngineStatusIcon.propTypes = {
  status: PropTypes.string,
  isInProgress: PropTypes.bool,
  style: PropTypes.object,
  type: PropTypes.string,
  count: PropTypes.number | undefined,
};

export default function EngineStatus(props) {
  const { engine, style, viewState } = props;
  if (!engine) return null;

  const isInProgress =
    viewState &&
    viewState.get("isInProgress") &&
    viewState.get("entityId") === engine.get("id");
  const status = engine.get("currentState");
  return (
    <EngineStatusIcon
      status={status}
      isInProgress={isInProgress}
      style={style}
    />
  );
}

EngineStatus.propTypes = {
  engine: PropTypes.instanceOf(Immutable.Map),
  style: PropTypes.object,
  viewState: PropTypes.instanceOf(Immutable.Map),
};
