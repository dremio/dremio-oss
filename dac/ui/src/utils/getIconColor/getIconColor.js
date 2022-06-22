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

import ICON_COLOR from "@app/constants/iconColors";

export const hashCode = (str) => {
  //Cast to a string in case of number input
  return String(str)
    .split("")
    .reduce(
      (prevHash, currVal) =>
        // eslint-disable-next-line no-bitwise
        ((prevHash << 5) - prevHash + currVal.charCodeAt(0)) | 0,
      0
    );
};

export const getIconColor = (id) => {
  let colorIndex;
  if (id == null) {
    colorIndex = 0;
  } else {
    colorIndex = Math.abs(hashCode(id)) % ICON_COLOR.length;
  }
  return ICON_COLOR[colorIndex];
};

export default getIconColor;
