/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

export const HISTORY_PANEL_SIZE = 30; // this is enough space so that the scroll bar on Windows will fit
export const TIME_DOT_DIAMETER = 8;
export const DEFAULT_HEIGHT_SQL = 171;
export const MARGIN_PANEL = 38;

export const CURVINESS = 80;
export const HISTORY_WIDTH = 55;
export const DEFAULT_GRAPH_WIDTH = 1200;
export const DEFAULT_GRAPH_OFFSET = 130;

export const DEFAULT_ROW_HEIGHT = 25;
export const DEFAULT_HEIGHT_TABLE = 1000;
export const MIN_COLUMN_WIDTH = 200;
export const HORIZONTAL_SCROLL_HEIGHT = 15;
export const MIN_TABLE_HEIGHT_WITH_SCROLL = DEFAULT_ROW_HEIGHT + HORIZONTAL_SCROLL_HEIGHT;

export const getMinTableHeight = (hasHorizontalScroll) =>
  hasHorizontalScroll ? MIN_TABLE_HEIGHT_WITH_SCROLL : DEFAULT_ROW_HEIGHT;
