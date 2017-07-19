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
import { PALE_NAVY } from './colors';

export const main = {
  width: '100%'
};

export const height = {
  height: '100%'
};

export const header = {
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
  height: 38,
  padding: '0 5px 0 10px',
  flexShrink: 0,
  background: PALE_NAVY
};

export const addButton = {
  marginTop: 0,
  marginRight: 0,
  marginBottom: 0,
  marginLeft: 'auto'
};

export const listContent = {
  ...height,
  padding: '0 10px'
};

export const listItem = {
  display: 'flex',
  alignItems: 'center'
};

export const link = {
  overflow: 'hidden',
  whiteSpace: 'nowrap',
  textOverflow: 'ellipsis',
  textDecoration: 'none',
  color: '#333',
  padding: '0 0 0 3px'
};
