/*
 * Copyright (C) 2017 Dremio Corporation
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
import { h3, body } from 'uiTheme/radium/typography';
import { BORDER_TABLE, CELL_EXPANSION_HEADER, WHITE } from 'uiTheme/radium/colors';

export const commonStyles = {
  header: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    height: 40,
    backgroundColor: CELL_EXPANSION_HEADER,
    borderTop: `1px solid ${BORDER_TABLE}`,
    borderLeft: `1px solid ${BORDER_TABLE}`,
    borderRight: `1px solid ${BORDER_TABLE}`,
    padding: '0 10px'
  },
  toggleLabel: {
    display: 'flex',
    ...h3,
    width: 400, // todo: clean up this hack
    marginLeft: -10
  },
  iconTheme: {
    Container: {
      margin: '-1px 10px 0 10px'
    }
  },
  toggle: {
    marginTop: 6,
    width: 'auto'
  },
  formText: {
    ...body,
    padding: 5,
    backgroundColor: WHITE
  },
  highlight: {
    backgroundColor: 'hsl(60, 94%, 76%)'
  }
};
