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
import { BLUE } from './colors';
import { bodySmall } from './typography';

export const button = {
  outline: 'none',
  textDecoration: 'none',
  border: '1px solid #D9D9D9',
  padding: '0 6px',
  position: 'relative',
  lineHeight: '30px',
  justifyContent: 'space-around',
  alignItems: 'center',
  textAlign: 'center',
  minWidth: 100,
  height: '32px',
  borderRadius: 4,
  marginRight: 5,
  fontSize: 13,
  cursor: 'pointer',
  display: 'flex'
};

export const primary = {
  ...button,
  color: '#fff',
  backgroundColor: BLUE,
  ':hover': {
    backgroundColor: '#68C6D3'
  },
  borderColor: BLUE
};

export const warn = {
  ...button,
  color: '#E46363',
  backgroundColor: '#FEEAEA',
  border: 'none'
};

export const outlined = {
  ...button,
  color: BLUE,
  backgroundColor: 'inherit',
  borderColor: BLUE
};


export const disabled = { // todo: DRY with text field
  ...button,
  color: '#B2B2B2',
  backgroundColor: '#DDD',
  borderColor: '#D9D9D9',
  cursor: 'default'
};


export const submitting = {
  primary: {
    backgroundColor: '#68C6D3',
    cursor: 'default'
  },
  secondary: {
    backgroundColor: 'rgba(0,0,0,0.02)',
    cursor: 'default'
  }
};

export const disabledLink = {
  color: '#B2B2B2',
  backgroundColor: 'rgba(0,0,0,0.08)',
  cursor: 'default',
  pointerEvents: 'none'
};

export const secondary = {
  ...button,
  color: '#333',
  backgroundColor: '#F2F2F2',
  ':hover': {
    backgroundColor: '#F9F9F9'
  }
};

export const mainHeader = {
  ...button,
  color: '#aaa',
  padding: '0 10px',
  backgroundColor: 'rgba(255,255,255,0.1)',
  borderLeft: '1px solid rgba(255,255,255,0.3)',
  borderTop: '1px solid rgba(255,255,255,0.3)',
  borderRight: '1px solid rgba(255,255,255,0.3)',
  borderBottom: '1px solid rgba(255,255,255,0.3)',
  ':hover': {
    color: '#eee',
    backgroundColor: 'rgba(255,255,255,0.2)'
  }
};

export const inline = {
  ...secondary,
  height: 24
};

export const sqlEditorButton = {
  ...bodySmall,
  height: 26,
  lineHeight: 'auto',
  backgroundColor: 'transparent',
  border: 0,
  minWidth: 60,
  marginRight: 5, // todo get rid of that
  borderRadius: 2,
  cursor: 'pointer',
  padding: '0 6px 0 0',
  marginBottom: 0
};
