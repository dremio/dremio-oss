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
import { formDescription } from "uiTheme/radium/typography";

export const borderDottedGreen = "1px dotted #92E2D0";
export const greenBackground = "var(--fill--brand)";
export const fieldAreaWidth = "240px";

export const areaWrap = {
  width: "100%",
  overflow: "auto",
};

export const fieldBox = {
  background: greenBackground,
  border: borderDottedGreen,
  borderRadius: "1px",
  width: "570px",
  height: "25px",
};

const defaultBorderWidth = {
  borderLeftWidth: "1px",
  borderRightWidth: "1px",
  borderTopWidth: "1px",
  borderBottomWidth: "1px",
};

export const dragAreaText = {
  width: 180,
  ...formDescription,
  textAlign: "center",
  display: "inline-block",
};

export const columnWrap = {
  width: "100%",
};

const dragArea = {
  base: {
    display: "flex",
    width: "100%",
    justifyContent: "center",
    alignItems: "center",
    flexWrap: "wrap",
    minHeight: 180,
    height: "100%",
    overflow: "auto",
    //padding: '2px 0',
    ...defaultBorderWidth,
  },
  empty: {
    background: "var(--fill--primary)",
    ...defaultBorderWidth,
  },
  notEmpty: {
    alignItems: "flex-start",
    background: "var(--fill--primary)",
  },
  grabbed: {
    alignItems: "top",
    background: greenBackground,
    ...defaultBorderWidth,
  },
};

export function getDragAreaStyle(isDragged, isEmpty) {
  if (isDragged) {
    return { ...dragArea.base, ...dragArea.grabbed };
  }
  if (isEmpty) {
    return { ...dragArea.base, ...dragArea.empty };
  }
  return { ...dragArea.base, ...dragArea.notEmpty };
}
