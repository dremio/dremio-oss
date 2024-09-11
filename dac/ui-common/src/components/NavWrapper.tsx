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

import type { FC, PropsWithChildren } from "react";

export const NavWrapper: FC<PropsWithChildren<{ nav: JSX.Element }>> = (
  props,
) => {
  return (
    <div
      className="flex flex-row overflow-hidden"
      style={{ blockSize: "100vh", inlineSize: "100vw" }}
    >
      <nav className="shrink-0" style={{ width: "64px" }}>
        {props.nav}
      </nav>
      <main className="h-full w-full overflow-hidden">{props.children}</main>
    </div>
  );
};
