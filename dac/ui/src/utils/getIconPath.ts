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

import dremioSpritePath from "dremio-ui-lib/dist-icons/dremio.svg";

export { dremioSpritePath };
export const iconBasePath = "/static/icons/dremio";

type SourceTypes = "svg" | "png";
export const getSrcPath = (name: string, src: SourceTypes = "svg") => {
  if (src === "svg") return getIconPath(name);
  else return `${dremioSpritePath as unknown as string}/${name}.${src}`;
};

export const getIconPath = (name: string) =>
  `${iconBasePath}/${name}.svg` as const;
