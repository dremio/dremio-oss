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

import { useState } from "react";
export enum Themes {
  LIGHT = "dremio-light",
  DARK = "dremio-dark",
  SYSTEM_AUTO = "dremio-auto-system",
}

export const getColorScheme = (): "light" | "dark" =>
  //@ts-ignore
  window.getComputedStyle(document.body)["color-scheme"];

const THEME_STORAGE_KEY = "theme";

const DEFAULT_THEME = Themes.LIGHT;

export const getTheme = (): Themes => {
  const theme = global.localStorage.getItem(THEME_STORAGE_KEY);

  if (!theme || !Object.values(Themes).includes(theme as Themes)) {
    return DEFAULT_THEME;
  }

  return theme as Themes;
};

export const setTheme = (theme: Themes): void => {
  global.localStorage.setItem(THEME_STORAGE_KEY, theme);
  applyTheme();
};

export const applyTheme = () => {
  const rootEl = document.querySelector(":root")!;

  // Remove any existing theme classes
  Object.values(Themes).forEach((theme) => {
    rootEl.classList.remove(theme);
  });

  if (getTheme() === Themes.SYSTEM_AUTO) {
    const prefersDark = window.matchMedia(
      "(prefers-color-scheme: dark)",
    ).matches;
    prefersDark
      ? rootEl.classList.add(Themes.DARK)
      : rootEl.classList.add(Themes.LIGHT);
  } else {
    rootEl.classList.add(getTheme());
  }
};

export const useColorScheme = () => {
  const [colorScheme, setColorScheme] = useState<"light" | "dark">(
    getColorScheme(),
  );
  window.addEventListener("color-scheme-change", () => {
    if (getColorScheme() === "dark") {
      setColorScheme("dark");
    } else {
      setColorScheme("light");
    }
  });
  return colorScheme;
};
