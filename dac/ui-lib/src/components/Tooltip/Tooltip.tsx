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
import React from "react";
import Tooltip, {
  TooltipProps as MuiTooltipProps,
} from "@mui/material/Tooltip";
import { FormattedMessage } from "react-intl";

import "./Tooltip.scss";

type TooltipProps = MuiTooltipProps & {
  type?: "tooltip" | "richTooltip";
};

const TooltipMui = ({
  arrow = true,
  enterDelay = 500,
  enterNextDelay = 500,
  title = "",
  children,
  type = "tooltip",
  ...props
}: TooltipProps) => {
  const classes = { tooltip: type };
  return (
    <Tooltip
      title={
        typeof title !== "string" ? (
          title
        ) : (
          <FormattedMessage id={title} defaultMessage={title} />
        )
      }
      classes={classes}
      enterDelay={enterDelay}
      enterNextDelay={enterNextDelay}
      arrow={arrow}
      {...props}
    >
      {children}
    </Tooltip>
  );
};

export default TooltipMui;
