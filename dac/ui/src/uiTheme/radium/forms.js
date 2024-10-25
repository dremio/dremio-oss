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
import typography from "./typography";

export const form = {
  height: "100%",
};

export const formBody = {
  padding: "20px 15px",
};

export const modalFormBody = {
  height: "100%",
  flexGrow: 1,
  minHeight: 0,
};

export const modalFormWrapper = {
  height: "100%",
};

export const modalForm = {
  display: "flex",
  flexDirection: "column",
  height: "100%",
};

export const section = {
  marginBottom: 20,
};

export const sectionTitle = {
  marginTop: 24,
  marginBottom: 24,
  fontSize: 16,
  fontWeight: 600,
};

export const subSectionTitle = {
  fontSize: 14,
  marginBottom: 6,
  fontWeight: "bold",
};

export const formRow = {
  marginBottom: 16,
};

export const label = {
  ...typography.formLabel,
  color: "var(--text--primary)",
  fontWeight: 400,
};

export const textInput = {
  ...typography.formDefault,
  borderRadius: "4px",
  padding: "8px",
  width: "310px",
};

export const textInputError = {
  border: "1px solid var(--color--danger--400)",
};

export const textInputDisabled = {
  ...typography.formDescription,
  background: "var(--fill--disabled)",
  border: "1px solid var(--border--neutral--solid)",
  color: "var(--text--disabled)",
};

export const textInputSmall = {
  height: "24px",
  width: "180px",
};

export const textArea = {
  ...textInput,
  height: "56px",
  width: "100%",
  display: "block",
  border: "1px solid var(--border--neutral--solid)",
};

export const description = {
  ...typography.formDescription,
  ...formRow,
  width: "630px",
};

export const divider = {
  borderTop: "1px solid var(--border--neutral)",
  borderBottom: "none",
  margin: "10px 0",
};

export const checkboxFocus = {
  outline: `1px dotted var(--border--focused)`,
};

// currently only used by Radio/Checkbox/Select:
export const fieldDisabled = {
  opacity: 0.67, // passes contrast checks
};

export default {
  label,
  textInput,
  textInputError,
  textInputDisabled,
  textInputSmall,
  textArea,
  description,
  divider,
  fieldDisabled,
};
