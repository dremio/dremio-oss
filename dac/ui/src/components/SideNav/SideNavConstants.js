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

// icon color
export const DEFAULT_ICON_COLOR = '#E5E5E5';
export const ACTIVE_ICON_COLOR = '#aadee5';

export const ICON_BACKGROUND_COLOR = [
  '#D02362',
  '#F79472',
  '#FFDCA7',
  '#3B84CB',
  '#63DAFF',
  '#9A51BF',
  '#CC88AE',
  '#8CA4E9',
  '#64C5BF',
  '#72D398'
];

export const dropdownMenuStyle =  {
  boxShadow: '0px 0px 16px rgba(0, 0, 0, 0.1)',
  overflow: 'visible',
  width: '254px'
};

export const menuListStyle = {
  float: 'left',
  position: 'relative',
  zIndex: 1,
  padding: '3px 0',
  overflow: 'hidden',
  width: '253px'
};

export const hashCode = (str = '') => {
  return str.split('').reduce((prevHash, currVal) =>
    // eslint-disable-next-line no-bitwise
    (((prevHash << 5) - prevHash) + currVal.charCodeAt(0)) | 0, 0);
};
