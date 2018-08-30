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
import FormUtils from 'utils/FormUtils/FormUtils';
import FormElementConfig from 'utils/FormUtils/FormElementConfig';
import FormSectionConfig from 'utils/FormUtils/FormSectionConfig';
import SourceFormJsonPolicy from 'utils/FormUtils/SourceFormJsonPolicy';
import CheckEnabledContainerWrapper from 'components/Forms/Wrappers/CheckEnabledContainerWrapper';

export default class CheckEnabledContainerConfig extends FormElementConfig {

  constructor(config, functionalElements) {
    super(config);
    if (config.container) {
      if (config.container.propName) {
        // container is simple element
        config.container = SourceFormJsonPolicy.joinConfigsAndConvertElementToObj(config.container, functionalElements);
      } else {
        // container is a section
        config.container = new FormSectionConfig(config.container, functionalElements);
      }
    }
    this._renderer = CheckEnabledContainerWrapper;
  }

  getRenderer() {
    return this._renderer;
  }

  getContainer() {
    return super.getConfig().container;
  }

  getFields() {
    return [this.getPropName()].concat(super.getConfig().container.getFields());
  }

  addInitValues(initValues, state) {
    const elementConfig = super.getConfig();
    initValues = FormUtils.addInitValue(initValues, elementConfig.propName, elementConfig.checkValue);
    return elementConfig.container.addInitValues(initValues);
  }

  addValidators(validations) {
    const {container} = super.getConfig();
    validations = container.addValidators(validations);
    return validations;
  }

}
