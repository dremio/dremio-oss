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
import { BORDER, BACKGROUND } from './colors';


export const panel = {
  position: 'absolute',
  background: BACKGROUND,
  top: 42,
  bottom: 2,
  right: 0,
  left: 'calc(60% - 4px)',
  transform: 'translateX(100%)',
  borderRight: `1px solid ${BORDER}`,
  borderTop: `1px solid ${BORDER}`,
  borderBottom: `1px solid ${BORDER}`,
  display: 'flex',
  flexDirection: 'column',
  flexWrap: 'nowrap'
};


export const activePanel = {
  transform: 'translateX(-17px)'
};
