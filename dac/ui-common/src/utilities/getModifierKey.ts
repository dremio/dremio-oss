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

export enum ModifierKey {
  CTRL = "CTRL",
  CMD = "CMD",
}

const isAppleDevice = () => {
  if ("userAgentData" in window.navigator) {
    //@ts-ignore
    return navigator.userAgentData.platform === "macOS";
  }

  /**
   * Although deprecated, this still works in non-HTTPS contexts
   * as well as non-Chromium browsers like Safari
   */
  return /Mac|iP/.test(navigator.platform);
};

/**
 * Get the platform's default modifier key
 */
export const getModifierKey = () => {
  if (isAppleDevice()) {
    return ModifierKey.CMD;
  }

  return ModifierKey.CTRL;
};

export const getModifierKeyState = () => {
  const modifierKey = getModifierKey();

  switch (modifierKey) {
    case ModifierKey.CMD:
      return "Meta";
    case ModifierKey.CTRL:
      return "Control";
  }
};
