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

/* eslint-disable react/prop-types */
import * as React from "react";
import { forwardRef, type PropsWithChildren } from "react";
import clsx from "clsx";

type TagProps = {
  className?: string;
};

export const Tag = forwardRef<HTMLSpanElement, PropsWithChildren<TagProps>>(
  (props, ref) => {
    const { className, children, ...rest } = props;
    return (
      <span {...rest} ref={ref} className={clsx("dremio-tag", className)}>
        {children}
      </span>
    );
  },
);
