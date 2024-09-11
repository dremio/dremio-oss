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
//@ts-ignore
import invariant from "invariant";
import clsx from "clsx";
import { Tooltip, type TooltipPlacement } from "./Tooltip/Tooltip";

type IconButtonProps = React.HTMLAttributes<HTMLButtonElement> & {
  as?: any;
  to?: string;
  className?: string;
  type?: "button" | "submit";
  tooltip?: JSX.Element | string;
  tooltipPlacement?: TooltipPlacement;
  tooltipPortal?: boolean;
  tooltipDelay?: number;
  tooltipInteractive?: boolean;
  "aria-label"?: string;
  onTooltipOpen?: () => void;
  onTooltipClose?: () => void;
};
function validateProps(props: IconButtonProps) {
  //@ts-ignore
  if (process.env.NODE_ENV === "production") return;

  const isValid =
    (!!props.tooltip && !!props["aria-label"]) ||
    (!props.tooltip && !props["aria-label"]);

  invariant(
    !isValid,
    "One of props 'tooltip' or 'aria-label' must be defined and not both.",
  );
}

export const IconButton: any = React.forwardRef<
  HTMLButtonElement,
  IconButtonProps
>((props, ref) => {
  const id = React.useId();
  const {
    as = "button",
    className,
    tooltip,
    tooltipPlacement,
    tooltipPortal,
    tooltipDelay,
    tooltipInteractive,
    "aria-label": ariaLabel,
    onTooltipOpen,
    onTooltipClose,
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
    "aria-labelledby": id,
    ref,
  });

  if (tooltip) {
    return (
      <Tooltip
        content={tooltip}
        placement={tooltipPlacement}
        onOpen={onTooltipOpen}
        onClose={onTooltipClose}
        portal={tooltipPortal}
        delay={tooltipDelay}
        interactive={tooltipInteractive}
        shouldWrapChildren
        id={id}
      >
        {ButtonElement}
      </Tooltip>
    );
  } else {
    return ButtonElement;
  }
});
