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
import {
  applyTheme as commonApplyTheme,
  setTheme as commonSetTheme,
  Themes,
} from "dremio-ui-common/appTheme";
import { configureDremioIcon, loadSvgSprite } from "dremio-ui-lib/components";
import { getSpritePath } from "#oss/utils/getIconPath";

const applySvgSprite = () => {
  return loadSvgSprite(getSpritePath())
    .then(() => configureDremioIcon())
    .catch((e) => {
      console.error(e);
    });
};

export const applyTheme = () => {
  commonApplyTheme();
  applySvgSprite();
  dispatchEvent(new CustomEvent("color-scheme-change"));
};

export const setTheme = (theme: Themes) => {
  commonSetTheme(theme);
  applySvgSprite();
};
