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

import FormElementConfig from "@app/utils/FormUtils/FormElementConfig";
import FormUtils from "./FormUtils";
import { TextField } from "@app/components/Fields";

export const getJSONElementOverrides = (
  elements: Record<string, any>,
  sourceType: string,
) =>
  elements.map((el: any) => ({
    ...el,
    propertyName: FormUtils.addFormPrefixToPropName(el.propertyName),
  }));

export const getFormElementConfig = (elementJson: Record<string, any>) => {
  return new FormElementConfig(elementJson);
};

export const getSecureFieldRenderer = () => {
  return TextField;
};

export const isValidSecret = () => true;
