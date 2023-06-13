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

type SectionMessageAppearance =
  | "information"
  | "success"
  | "warning"
  | "danger"
  | "discovery";

type Props = {
  appearance: SectionMessageAppearance;
  children?: JSX.Element | JSX.Element[] | string;
  title?: JSX.Element | string;
} & React.HTMLAttributes<HTMLDivElement>;

const appearanceIcons: Record<SectionMessageAppearance, string> = {
  information: "interface/warning",
  success: "engine-state/running-engine",
  warning: "interface/warning",
  danger: "engine-state/stopped",
  discovery: "",
};

export const SectionMessage = forwardRef<HTMLDivElement, Props>(
  (props, ref): JSX.Element => {
    const { appearance, className, children, title, ...rest } = props;

    return (
      <section
        ref={ref}
        role="alert"
        {...rest}
        className={clsx(
          "dremio-section-message",
          `dremio-section-message--${appearance}`,
          className
        )}
      >
        <div className="dremio-section-message__icon">
          {/*@ts-ignore*/}
          <dremio-icon name={appearanceIcons[appearance]} alt=""></dremio-icon>
        </div>
        <div className="dremio-section-message__content">
          {title && (
            <header className="dremio-section-message__header">{title}</header>
          )}
          {children}
        </div>
      </section>
    );
  }
);
