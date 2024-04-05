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
/* eslint-disable react/prop-types */
import * as React from "react";
import { forwardRef } from "react";
import clsx from "clsx";

type Props = {
  renderButton?: (
    show: boolean,
    setShow: (newVal: boolean) => void,
  ) => JSX.Element;
  message: JSX.Element | string;
  details?: JSX.Element | string;
  show?: boolean; // Initialize shown
} & React.HTMLAttributes<HTMLDivElement>;

/**
 * For use with SectionMessage, MessageDetails provides a way to progressively disclose error/warning/info details to the user.
 */

export const MessageDetails = forwardRef<HTMLDivElement, Props>(
  (props, ref): JSX.Element => {
    const {
      className,
      renderButton = (show: boolean, setShow: (newVal: boolean) => void) => {
        const onClick = (e: any) => {
          e.preventDefault();
          e.stopPropagation();
          setShow(!show);
        };
        return (
          <a
            href="#"
            role="button"
            onClick={onClick}
            style={{ whiteSpace: "nowrap" }}
          >
            {show ? "Show less" : "Show more"}
          </a>
        );
      },
      message,
      details,
      show: showProp = false,
      ...rest
    } = props;
    const [show, setShow] = React.useState(showProp);

    return (
      <div
        ref={ref}
        role="alert"
        {...rest}
        className={clsx("dremio-message-details", className)}
      >
        <div className="dremio-message-details__content">
          <div className="dremio-message-details__message">
            {message}
            {details && (
              <span
                style={{ marginLeft: 8 }}
                className="dremio-message-details_button"
              >
                {renderButton(show, setShow)}
              </span>
            )}
          </div>
          {show && (
            <div
              className="dremio-message-details__details"
              style={{ marginTop: 4 }}
            >
              {details}
            </div>
          )}
        </div>
      </div>
    );
  },
);
