/* eslint-disable react/prop-types */
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
import clsx from "clsx";
import { forwardRef, type ReactNode } from "react";
import { Spinner } from "../Spinner/Spinner";
import { Tooltip, type TooltipPlacement } from "../Tooltip/Tooltip";

type ButtonProps = {
  as?: string | React.FunctionComponent<any> | React.ComponentClass<any, any>;

  /**
   * Primary content of the button
   */
  children: ReactNode;

  /**
   * Optional content aligned against the starting edge
   */
  prefix?: ReactNode;

  /**
   * Optional content aligned against the ending edge
   */
  suffix?: ReactNode;

  /**
   * Disables the button and shows an indiciator for pending state
   */
  pending?: boolean;

  /**
   * Keeps the button disabled after a pending state but restores original content
   */
  success?: boolean;

  /**
   * If defined, a tooltip will be added to the button
   */
  tooltip?: string;

  /**
   * If tooltip is defined, this will define where the tooltip is placed
   */
  tooltipPlacement?: TooltipPlacement;

  /**
   * The visual style of the button
   */
  variant:
    | "primary"
    | "secondary"
    | "tertiary"
    | "primary-danger"
    | "secondary-danger"
    | "tertiary-danger";

  className?: string;
} & React.DetailedHTMLProps<
  React.ButtonHTMLAttributes<HTMLButtonElement>,
  HTMLButtonElement
>;

const getContent = (buttonProps: ButtonProps): ReactNode => {
  if (buttonProps.pending) {
    return <Spinner />;
  }

  return buttonProps.children;
};

const isDisabled = (buttonProps: ButtonProps): boolean =>
  buttonProps.disabled || buttonProps.pending || buttonProps.success || false;

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  (props, ref) => {
    const {
      as = "button",
      prefix,
      suffix,
      variant,
      pending,
      type = "button",
      tooltip,
      tooltipPlacement,
      ...rest
    } = props;
    const buttonProps =
      as === "button"
        ? {
            "aria-busy": pending ? "true" : "false",
            "aria-live": "assertive",
            disabled: isDisabled(props),
            type,
          }
        : {};
    return React.createElement(
      as,
      {
        ...rest,
        className: clsx(
          "dremio-button",
          `dremio-button--${variant}`,
          props.className,
        ),
        ...buttonProps,
        ref,
      },
      <>
        {prefix && <div className="dremio-button__prefix">{prefix}</div>}
        {tooltip ? (
          <Tooltip
            content={tooltip}
            shouldWrapChildren
            placement={tooltipPlacement}
          >
            <div className="dremio-button__content">{getContent(props)}</div>
          </Tooltip>
        ) : (
          <div className="dremio-button__content">{getContent(props)}</div>
        )}
        {suffix && <div className="dremio-button__suffix">{suffix}</div>}
      </>,
    );
  },
);
