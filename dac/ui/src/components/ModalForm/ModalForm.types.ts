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

import { HTMLInputTypeAttribute } from "react";

export type FormComponent = {
  key: string;
  formInputType: ElementTypes;
  label?: string;
  typeProps?: FormTypeProps;
  components?: FormComponent[];
  helpText?:
    | string
    | { dependencies: string[]; getText: (...formValues: any) => string };
  tooltipHint?: string;
  placeholder?: string;
  className?: string;
  sectionLabel?: string;
  renderCustomComponent?: (args: FormCustomProps) => JSX.Element;
  noLabel?: boolean;
};

export type ModalFormTabs = {
  label: string;
  contentOutline: FormComponent[];
}[];

export type ElementTypes =
  | "input"
  | "multi"
  | "select"
  | "textarea"
  | "checkbox"
  | "checkbox-subsection"
  | "custom";

export type FormTypeProps = { defaultValue?: any; disabled?: boolean } & (
  | InputTypeProps
  | SelectTypeProps
  | TextAreaTypeProps
);

export type FormCustomProps = {
  element: FormComponent;
  disabled: boolean;
};

export type InputTypeProps = {
  type: HTMLInputTypeAttribute;
  min?: number;
  max?: number;
};

export type TextAreaTypeProps = {
  rows?: number;
};

export type SelectTypeProps = {
  options: { value: string; label: string }[];
};
