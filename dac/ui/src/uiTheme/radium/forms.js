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
import { WARNING, GREY } from 'uiTheme/radium/colors';
import typography from './typography';

export const form = {
  height: '100%'
};

export const formBody = {
  padding: '20px 15px'
};

export const modalFormBody = {
  height: '100%',
  flexGrow: 1,
  minHeight: 0
};

export const modalFormWrapper = {
  height: '100%'
};

export const modalForm = {
  display: 'flex',
  flexDirection: 'column',
  height: '100%'
};

export const section = {
  marginBottom: 20
};

export const sectionTitle = {
  marginBottom: 6
};

export const subSectionTitle = {
  fontSize: 14,
  marginBottom: 6
};

export const formRow = {
  marginBottom: 10
};

export const label = {
  marginBottom: 0,
  display: 'block',
  ...typography.formLabel
};

export const textInput = {
  ...typography.formDefault,
  paddingLeft: '5px',
  border: '1px solid rgba(0,0,0,0.1)',
  borderRadius: '2px',
  height: '28px',
  width: '310px',
  ':focus': {
    border: '1px solid #C0E9F5',
    boxShadow: '0 0 4px 0 rgba(129,209,235,0.20)'
  }
};

export const textInputError = {
  background: 'rgba(253, 186, 186, 0.15)',
  border: `1px solid ${WARNING}`
};

export const textInputDisabled = {
  ...typography.formDescription,
  background: '#E8E8E8',
  border: '1px solid #E9E9E9',
  color: textInput.color,
  opacity: 0.67
};

export const textInputSmall = {
  height: '24px',
  width: '180px'
};

export const textArea = {
  ...textInput,
  height: '56px',
  width: '100%',
  display: 'block'
};

export const description = {
  ...typography.formDescription,
  ...formRow,
  width: '630px'
};

export const divider = {
  borderTop: '1px solid rgba(0,0,0,0.1)',
  borderBottom: 'none',
  margin: '10px 0'
};

export const checkboxFocus = {
  outline: `1px dotted ${GREY}`
};

// currently only used by Radio/Checkbox/Select:
export const fieldDisabled = {
  opacity: 0.67 // passes contrast checks
};

export default {
  label, textInput, textInputError, textInputDisabled, textInputSmall, textArea, description, divider, fieldDisabled
};
