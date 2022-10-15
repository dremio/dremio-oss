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

type DialogContentProps = {
  actions?: JSX.Element[] | JSX.Element;
  className?: string;
  children: JSX.Element | JSX.Element[];
  title?: string;
  icon?: JSX.Element;
  toolbar?: JSX.Element;
};

export const DialogContent = forwardRef<HTMLDivElement, DialogContentProps>(
  (props, ref) => {
    return (
      <div className={clsx("dremio-dialog-content", props.className)} ref={ref}>
        <header className="dremio-dialog-content__header">
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
        <div className="dremio-dialog-content__main">{props.children}</div>
        <footer className="dremio-dialog-content__footer">
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
