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

import { forwardRef } from "react";

export const CatalogPath = forwardRef<
  HTMLSpanElement,
  JSX.IntrinsicElements["span"] & { path: string }
>(function CatalogPath(props, ref) {
  const { path, style, ...rest } = props;
  return (
    <span
      {...rest}
      ref={ref}
      style={{
        whiteSpace: "pre",
        userSelect: "all",
        textOverflow: "ellipsis",
        overflow: "hidden",
        display: "inline-block",
        ...style,
      }}
    >
      {path}
    </span>
  );
});
