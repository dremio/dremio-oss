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
  message?: JSX.Element | string;
  details?: JSX.Element | string;
  show?: boolean; // Initialize shown
  showCaret?: boolean;
} & React.HTMLAttributes<HTMLDivElement>;

/**
 * MessageDetails provides a way to progressively disclose details to the user.
 */

export const MessageDetails = forwardRef<HTMLDivElement, Props>(
  (props, ref): JSX.Element => {
    const {
      className,
      message,
      details,
      show: showProp = false,
      showCaret,
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
            {message && <>{message}</>}
            {details && (
              <span
                className={clsx("dremio-message-details_button", {
                  "ml-1": !!message,
                })}
              >
                <a
                  href="#"
                  role="button"
                  onClick={(e: any) => {
                    e.preventDefault();
                    e.stopPropagation();
                    setShow(!show);
                  }}
                  style={{ whiteSpace: "nowrap" }}
                >
                  {show ? "Show less" : "Show more"}
                  {showCaret &&
                    (show ? ( //@ts-ignore
                      <dremio-icon
                        alt="Show less"
                        name="interface/caretUp"
                        //@ts-ignore
                      ></dremio-icon>
                    ) : (
                      //@ts-ignore
                      <dremio-icon
                        alt="Show more"
                        name="interface/caretDown"
                        //@ts-ignore
                      ></dremio-icon>
                    ))}
                </a>
              </span>
            )}
          </div>
          {show && (
            <div className="dremio-message-details__details mt-05">
              {details}
            </div>
          )}
        </div>
      </div>
    );
  },
);
