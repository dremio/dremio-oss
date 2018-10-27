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
import { BLUE } from './colors';
import { bodySmall } from './typography';

export const button = {
  outline: 'none !important',
  textDecoration: 'none',
  borderRight: 'none',
  borderLeft: 'none',
  borderTop: 'none',
  padding: '0 6px',
  position: 'relative',
  lineHeight: '28px',
  justifyContent: 'space-around',
  alignItems: 'center',
  textAlign: 'center',
  minWidth: 100,
  height: 28,
  borderRadius: 2,
  marginRight: 5,
  fontSize: 11,
  cursor: 'pointer'
};

export const primary = {
  ...button,
  color: '#fff',
  backgroundColor: BLUE,
  borderBottom: '1px solid #3399A8',
  ':hover': {
    backgroundColor: '#68C6D3'
  }
};

export const disabled = { // todo: DRY with text field
  ...button,
  color: '#B2B2B2',
  backgroundColor: '#DDD',
  cursor: 'default',
  borderBottom: 'none'
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
  borderBottom: 'none',
  pointerEvents: 'none'
};

export const secondary = {
  ...button,
  color: '#333',
  backgroundColor: 'rgba(0,0,0,0.04)',
  borderBottom: '1px solid rgba(0,0,0,0.05)',
  ':hover': {
    backgroundColor: 'rgba(0,0,0,0.02)'
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
  backgroundColor: 'transparent',
  borderBottom: 'none',
  borderTop: 'none',
  borderLeft: 'none',
  borderRight: 'none',
  minWidth: 60,
  marginRight: 5, // todo get rid of that
  borderRadius: 2,
  cursor: 'pointer',
  padding: '0 6px 0 0',
  marginBottom: 0
};
