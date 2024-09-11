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

export const panel = {
  position: "absolute",
  background: "var(--color--neutral--50)",
  top: 42,
  bottom: 2,
  right: 0,
  left: "calc(60% - 4px)",
  transform: "translateX(100%)",
  borderRight: `1px solid var(--color--neutral--200)`,
  borderTop: `1px solid var(--color--neutral--200)`,
  borderBottom: `1px solid var(--color--neutral--200)`,
  display: "flex",
  flexDirection: "column",
  flexWrap: "nowrap",
};

export const activePanel = {
  transform: "translateX(-17px)",
};
