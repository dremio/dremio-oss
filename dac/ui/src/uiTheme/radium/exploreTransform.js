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
import {formLabel} from 'uiTheme/radium/typography';

export const methodTitle = {
  margin: '8px 0 10px 8px',
  padding: '0 5px 0 5px',
  lineHeight: '24px',
  height: 24
};

export const methodTab = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  margin: '8px 0 10px 8px',
  padding: '0 5px 0 5px',
  height: 24,
  minWidth: 40,
  borderRadius: 2,
  color: 'black',
  float: 'left',
  cursor: 'pointer',
  ':hover': {
    backgroundColor: 'rgba(0,0,0,0.05)'
  }
};

export const formSectionTitle = {
  ...formLabel,
  marginBottom: 10
};

export const cardsWrap = {
  paddingBottom: 10,
  display: 'flex',
  overflowX: 'auto'
};

export const inlineLabel = {
  ...formLabel,
  display: 'block',
  width: 60
};

export const inlineFieldWrap = {
  marginLeft: 10,
  marginTop: 5,
  display: 'flex',
  alignItems: 'center'
};

