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

type SectionProps = {
  children: ReactNode;
  title?: JSX.Element;
  toolbar?: JSX.Element;

  className?: string;
};

export const Section = forwardRef<HTMLDivElement, SectionProps>(
  (props, ref) => {
    const { children, className, title, toolbar, ...rest } = props;
    return (
      <section
        ref={ref}
        {...rest}
        className={clsx("dremio-section", className)}
      >
        <header className="dremio-section__header">
          {title && <span className="dremio-section__title">{title}</span>}
          {toolbar && <div className="dremio-section__toolbar">{toolbar}</div>}
        </header>
        <div className="dremio-section__body">{children}</div>
      </section>
    );
  },
);
