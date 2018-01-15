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
import typography from './typography';

export const title = {
  margin: '0 0 5px 10px'
};

export const cardTitle = {
  ...typography.h5,
  margin: '0 0 10px 0'
};


export const contextCard = {
  padding: 10,
  ':hover': {
    backgroundColor: '#fff'
  }
};

export const contextAttrs = {
  marginBottom: 10
};

export const attrLabel = {
  ...typography.keyLabel,
  display: 'inline-block',
  width: 100
};

export const attrValue = {
  ...typography.valueLabel
};
