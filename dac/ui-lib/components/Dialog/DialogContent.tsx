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
import { forwardRef } from "react";
import clsx from "clsx";
import { useDetectScroll } from "../utilities/useDetectScroll";

export type DialogContentProps = {
  actions?: JSX.Element[] | JSX.Element;
  className?: string;
  children: JSX.Element | JSX.Element[];
  error?: JSX.Element;
  expanded?: boolean;
  title?: JSX.Element | string;
  icon?: JSX.Element;
  toolbar?: JSX.Element;
};

/**
 * DialogContent is a building block for more complete patterns in dremio-ui-common
 * such as MessageDialog and ConfirmationDialog.
 */
export const DialogContent = forwardRef<HTMLDivElement, DialogContentProps>(
  (props, ref) => {
    const { scrolledDirections, scrollContainerRef } = useDetectScroll([
      "top",
      "bottom",
    ]);
    return (
      <div
        className={clsx("dremio-dialog-content", props.className, {
          "dremio-dialog-content--expanded": props.expanded,
        })}
        ref={ref}
      >
        <div className="dremio-dialog-content-notifications"></div>
        <header
          className={clsx(
            "dremio-dialog-content__header dremio-scroll-shadow dremio-scroll-shadow--top",
            { "--scrolled": scrolledDirections.has("top") }
          )}
        >
          {props.icon && (
            <span className="dremio-dialog-content__header-icon">
              {props.icon}
            </span>
          )}
          {props.title && (
            <h1 className="dremio-dialog-content__header-title">
              {props.title}
            </h1>
          )}
          {props.toolbar && (
            <div className="dremio-dialog-content__header-toolbar">
              {props.toolbar}
            </div>
          )}
        </header>
        {props.error && (
          <div className="dremio-dialog-content__error">{props.error}</div>
        )}
        <div ref={scrollContainerRef} className="dremio-dialog-content__main">
          {props.children}
        </div>
        <footer
          className={clsx(
            "dremio-dialog-content__footer",
            "dremio-scroll-shadow dremio-scroll-shadow--bottom",
            {
              "--scrolled": scrolledDirections.has("bottom"),
            }
          )}
        >
          {props.actions && (
            <div className="dremio-dialog-content__footer-actions">
              <div className="dremio-button-group">{props.actions}</div>
            </div>
          )}
        </footer>
      </div>
    );
  }
);
