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
import { forwardRef } from "react";

type ShellProps = {
  children?: JSX.Element;
  className?: string;
  navItems?: JSX.Element[];
};

export const Shell = forwardRef<HTMLDivElement, ShellProps>((props, ref) => {
  const { children, className, ...rest } = props;
  return (
    <div ref={ref} {...rest} className={clsx("dremio-shell", className)}>
      <nav className="dremio-shell__nav" aria-label="Main">
        <ul></ul>
      </nav>
      <main className="dremio-shell__main">{children}</main>
    </div>
  );
});
