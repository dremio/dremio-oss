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

import { forwardRef, PropsWithChildren } from "react";

export const FilterTag = forwardRef<
  HTMLButtonElement,
  PropsWithChildren<{
    "aria-pressed": boolean;
    count?: number;
    onClick: () => void;
  }>
>(function FilterTag(props, ref) {
  return (
    <button
      className="bg-primary border-thin inline-flex items-center flex-row py-05 px-1 gap-05 h-full"
      ref={ref}
      onClick={props.onClick}
      type="button"
      aria-pressed={props["aria-pressed"]}
      style={{
        borderRadius: "4px",
        borderWidth: "1.25px",
        whiteSpace: "nowrap",
        ...(props["aria-pressed"]
          ? {
              background: "var(--fill--brand)",
              borderColor: "var(--border--brand--solid)",

              color: "var(--text--brand)",
            }
          : {}),
      }}
    >
      {props.children} {props.count ? `(${props.count})` : null}
    </button>
  );
});
