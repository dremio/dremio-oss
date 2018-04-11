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
export const FLEX_WRAPPER = {
  display: 'flex',
  flex: 1,
  minHeight: 0
};

export const FLEX_COL_START = {
  display: 'flex',
  flexDirection: 'column',
  justifyContent: 'flex-start'
};

export const FLEX_START_WRAP = {
  display: 'flex',
  justifyContent: 'flex-start',
  flexWrap: 'wrap'
};

export const FLEX_COL_BETWEEN = {
  display: 'flex',
  flexDirection: 'column',
  justifyContent: 'space-between'
};

export const FLEX_COL_AROUND = {
  display: 'flex',
  flexDirection: 'column',
  justifyContent: 'space-around'
};

export const CENTER = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  height: '100%'
};

export const FLEX_WRAP_COL_START = {
  display: 'flex',
  flexWrap: 'wrap',
  flexDirection: 'column',
  justifyContent: 'flex-start'
};

export const FLEX_WRAP_COL_START_CENTER = {
  display: 'flex',
  flexWrap: 'wrap',
  flexDirection: 'column',
  justifyContent: 'flex-start',
  alignItems: 'center'
};

export const FLEX_WRAP_COL_CENTER = {
  display: 'flex',
  flexWrap: 'wrap',
  flexDirection: 'column',
  justifyContent: 'center'
};

export const LINE_START_CENTER = {
  display: 'inline-flex',
  justifyContent: 'flex-start',
  alignItems: 'center'
};

export const LINE_CENTER_CENTER = {
  display: 'inline-flex',
  justifyContent: 'center',
  alignItems: 'center'
};

export const LINE_START_BASE = {
  display: 'inline-flex',
  justifyContent: 'flex-start',
  alignItems: 'baseline'
};

export const LINE_START_START = {
  display: 'inline-flex',
  justifyContent: 'flex-start',
  alignItems: 'flex-start'
};

export const LINE_ROW_START_CENTER = {
  display: 'inline-flex',
  flexDirection: 'row',
  justifyContent: 'flex-start',
  alignItems: 'center'
};

export const LINE_NOROW_START_STRETCH = {
  display: 'inline-flex',
  flexDirection: 'norow',
  justifyContent: 'flex-start',
  alignItems: 'stretch'
};

export const LINE_NOWRAP_ROW_START_CENTER = {
  display: 'inline-flex',
  flexWrap: 'nowrap',
  flexDirection: 'row',
  justifyContent: 'flex-start',
  alignItems: 'center'
};

export const LINE_WRAP_ROW_START_CENTER = {
  display: 'inline-flex',
  flexWrap: 'wrap',
  flexDirection: 'row',
  justifyContent: 'flex-start',
  alignItems: 'center'
};

export const LINE_NOWRAP_BETWEEN = {
  display: 'inline-flex',
  flexDirection: 'row',
  justifyContent: 'space-between'
};

export const LINE_NOWRAP_ROW_BETWEEN_CENTER = {
  display: 'inline-flex',
  flexWrap: 'nowrap',
  flexDirection: 'row',
  justifyContent: 'space-between',
  alignItems: 'center'
};

export const FLEX_NOWRAP_ROW_BETWEEN_CENTER = {
  display: 'flex',
  flexWrap: 'nowrap',
  flexDirection: 'row',
  justifyContent: 'space-between',
  alignItems: 'center'
};

export const LINE_NOWRAP_ROW_AROUND = {
  display: 'inline-flex',
  flexWrap: 'nowrap',
  flexDirection: 'row',
  justifyContent: 'space-around'
};

export const FLEX_NOWRAP_ROW_SPACE_BETWEEN_START = {
  display: 'flex',
  flexWrap: 'nowrap',
  flexDirection: 'row',
  justifyContent: 'space-between',
  alignItems: 'flex-start'
};

export const FLEX_NOWRAP_ROW_SPACE_BETWEEN_CENTER = {
  display: 'flex',
  flexWrap: 'nowrap',
  flexDirection: 'row',
  justifyContent: 'space-between',
  alignItems: 'center'
};

export const FLEX_NOWRAP_COL_ALIGN_START = {
  display: 'flex',
  flexWrap: 'nowrap',
  flexDirection: 'column',
  alignItems: 'flex-start'
};

export const INLINE_NOWRAP_ROW_FLEX_START = {
  display: 'inline-flex',
  flexWrap: 'nowrap',
  flexDirection: 'row',
  alignItems: 'flex-start'
};

export const FLEX_NOWRAP_COL_START = {
  display: 'flex',
  flexWrap: 'nowrap',
  flexDirection: 'column',
  justifyContent: 'flex-start'
};

export const FLEX_COL_END_CENTER = {
  display: 'flex',
  flexDirection: 'column',
  justifyContent: 'center',
  alignItems: 'center'
};

export const FLEX_COL_START_START = {
  display: 'flex',
  flexDirection: 'column',
  justifyContent: 'flex-start',
  alignItems: 'flex-start'
};

export const FLEX_NOWRAP_CENTER_START = {
  display: 'flex',
  flexDirection: 'nowrap',
  justifyContent: 'center',
  alignItems: 'flex-start'
};

export const FLEX_ROW_START_CENTER = {
  display: 'flex',
  flexDirection: 'row',
  justifyContent: 'flex-start',
  alignItems: 'center'
};

export default {
  CENTER,
  FLEX_COL_START,
  FLEX_WRAP_COL_START,
  LINE_START_CENTER,
  FLEX_WRAP_COL_CENTER,
  LINE_ROW_START_CENTER,
  FLEX_NOWRAP_COL_START,
  LINE_START_BASE,
  FLEX_COL_BETWEEN,
  FLEX_NOWRAP_COL_ALIGN_START,
  LINE_NOWRAP_BETWEEN,
  LINE_NOWRAP_ROW_BETWEEN_CENTER,
  LINE_NOWRAP_ROW_AROUND,
  LINE_CENTER_CENTER,
  LINE_NOWRAP_ROW_START_CENTER,
  FLEX_COL_END_CENTER,
  LINE_WRAP_ROW_START_CENTER,
  FLEX_COL_AROUND,
  INLINE_NOWRAP_ROW_FLEX_START,
  LINE_NOROW_START_STRETCH,
  FLEX_COL_START_START,
  FLEX_START_WRAP,
  FLEX_NOWRAP_ROW_BETWEEN_CENTER,
  FLEX_NOWRAP_ROW_SPACE_BETWEEN_START,
  FLEX_NOWRAP_CENTER_START,
  FLEX_WRAP_COL_START_CENTER,
  FLEX_ROW_START_CENTER,
  LINE_START_START,
  FLEX_NOWRAP_ROW_SPACE_BETWEEN_CENTER
};
