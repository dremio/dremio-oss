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
/* eslint-disable */
import fs from "fs";
import glob from "glob";
import path from "path";
import { fileURLToPath } from "url";
const __dirname = path.dirname(fileURLToPath(import.meta.url));

const ICON_ROOT = path.join(__dirname, "../icons");

const toIconManifest = (iconPath) => {
  const icon = iconPath.substring(ICON_ROOT.length + 1);
  const [theme, ...rest] = icon.split("/");

  return {
    theme,
    name: rest.join("/").replace(".svg", ""),
    path: path.join("icons", icon),
  };
};

fs.writeFileSync(
  path.join(__dirname, "../example/manifest.json"),
  JSON.stringify(
    glob.sync(`${ICON_ROOT}/**/*.svg`).map(toIconManifest),
    null,
    2
  ) + "\n"
);
