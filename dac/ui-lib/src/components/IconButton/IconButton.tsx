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

import * as React from "react";
import invariant from "invariant";
import clsx from "clsx";
import Tooltip from "../Tooltip";

type IconButtonProps =
  | (React.HTMLAttributes<HTMLButtonElement> & {
      as?: any;
      className?: string;
      type?: "button" | "submit";
    }) & {
      tooltip: React.ReactNode;
      tooltipPlacement: string;
      "aria-label"?: void;
    } & {
      tooltip?: void;
      tooltipPlacement?: void;
      "aria-label": string;
    };

function validateProps(props: IconButtonProps) {
  //@ts-ignore
  if (process.env.NODE_ENV === "production") return;

  const isValid =
    (!!props.tooltip && !!props["aria-label"]) ||
    (!props.tooltip && !props["aria-label"]);

  invariant(
    !isValid,
    "One of props 'tooltip' or 'aria-label' must be defined and not both."
  );
}

export const IconButton: React.FC<IconButtonProps> = (props) => {
  const {
    as = "button",
    className,
    tooltip,
    tooltipPlacement,
    "aria-label": ariaLabel,
    ...rest
  } = props;

  validateProps(props);

  const defaultTypeProp =
    as === "button" && !props.type ? { type: "button" } : {};

  const ButtonElement = React.createElement(as, {
    ...defaultTypeProp,
    ...rest,
    className: clsx(className, "dremio-icon-button"),
    tabIndex: 0,
    "aria-label": ariaLabel,
  });

  if (tooltip) {
    return (
      <Tooltip title={tooltip} placement={tooltipPlacement}>
        {ButtonElement}
      </Tooltip>
    );
  } else {
    return ButtonElement;
  }
};
