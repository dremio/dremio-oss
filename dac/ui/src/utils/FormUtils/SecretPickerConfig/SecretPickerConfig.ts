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
import { SecretPickerWrapper } from "@inject/components/Forms/Wrappers/SecretPickerWrapper";
import { ElementConfigJSON } from "@app/types/Sources/SourceFormTypes";
import FormElementConfig from "@app/utils/FormUtils/FormElementConfig";
import FormUtils from "../FormUtils";
import { getDefaultValue } from "@inject/utils/FormUtils/SecretPickerConfig/utils";

export default class SecretPickerConfig extends FormElementConfig {
  constructor(props: ElementConfigJSON) {
    super(props);
  }

  getRenderer() {
    return SecretPickerWrapper;
  }

  addInitValues(initValues: any, state: any, props: any) {
    const elementConfig = super.getConfig();
    initValues = FormUtils.addInitValue(
      initValues,
      elementConfig.propName,
      getDefaultValue(elementConfig),
      undefined,
    );
    return initValues;
  }

  addValidators(validations: any) {
    return validations;
  }
}
