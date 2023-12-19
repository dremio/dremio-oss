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

import clsx from "clsx";
import * as React from "react";

import { Tooltip, type TooltipPlacement } from "./Tooltip/Tooltip";

type LabelProps = {
  value: JSX.Element | string;
  helpText?: JSX.Element | string;
  tooltipPlacement?: TooltipPlacement;
  helpVariant?: "tooltip" | "inline";
  classes?: { root?: string; label?: string; icon?: string; help?: string };
};

export const Label = (props: LabelProps) => {
  const {
    value,
    helpText,
    classes,
    tooltipPlacement,
    helpVariant = "tooltip",
  } = props;

  const renderHelpText = () => {
    if (!helpText) {
      return;
    }

    switch (helpVariant) {
      case "inline":
        return (
          <span
            className={clsx(
              classes?.help,
              "margin-top--half",
              "dremio-label__help"
            )}
          >
            {helpText}
          </span>
        );
      case "tooltip":
      default:
        return (
          <Tooltip content={helpText} placement={tooltipPlacement}>
            {/* @ts-ignore */}
            <dremio-icon
              name="interface/information"
              class={clsx(
                classes?.icon,
                "dremio-label__helpIcon margin-left--half"
              )}
            />
          </Tooltip>
        );
    }
  };

  return (
    <div
      className={clsx(
        classes?.root,
        "dremio-label flex",
        helpVariant === "tooltip" ? "--alignCenter" : "dremio-label-inline"
      )}
    >
      <div className={clsx(classes?.label, "dremio-label__text")}>{value}</div>
      {renderHelpText()}
    </div>
  );
};
