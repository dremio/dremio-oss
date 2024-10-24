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

import { FC, PropsWithChildren } from "react";

export const PageTop: FC<PropsWithChildren> = (props) => {
  return (
    <div
      id="page-top"
      className="bg-primary flex flex-row items-center px-1 shrink-0"
      style={{
        borderBottom: "1px solid var(--border--color)",
        height: "40px",
        // Temporary workaround for pages with scroll overflow issues
        position: "sticky",
        top: 0,
      }}
    >
      {props.children}
    </div>
  );
};
