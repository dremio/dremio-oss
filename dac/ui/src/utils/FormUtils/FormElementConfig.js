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
import TextWrapper from 'components/Forms/Wrappers/TextWrapper';
import TextareaWrapper from 'components/Forms/Wrappers/TextareaWrapper';
import CheckboxWrapper from 'components/Forms/Wrappers/CheckboxWrapper';
import SelectWrapper from 'components/Forms/Wrappers/SelectWrapper';
import RadioWrapper from 'components/Forms/Wrappers/RadioWrapper';
import DurationWrapper from 'components/Forms/Wrappers/DurationWrapper';
import ByteWrapper from 'components/Forms/Wrappers/ByteWrapper';
import SqlWrapper from 'components/Forms/Wrappers/SqlWrapper';

/**
 * Base class for configuration of complex form elements and used as is for simple elements
 */
export default class FormElementConfig {
  constructor(config) {
    this._config = config || {};
    this._foundInFunctionalConfig = config.foundInFunctionalConfig;
    this._renderer = FormElementConfig.getRenderer(config.type);
  }

  static getRenderer(type) {
    // Because this config class is used for several simple element types,
    // the renderer component is selected based on the element type
    switch (type) {
      /* eslint-disable indent */
      case 'text':
      case 'number':
        return TextWrapper;
      case 'textarea':
        return TextareaWrapper;
      case 'checkbox':
        return CheckboxWrapper;
      case 'select':
        return SelectWrapper;
      case 'radio':
        return RadioWrapper;
      case 'duration':
        return DurationWrapper;
      case 'byte':
        return ByteWrapper;
      case 'sql':
        return SqlWrapper;
      default:
        return TextWrapper;
      /* eslint-enable indent */
    }
  }

  getConfig() {
    return this._config;
  }

  getType() {
    return this._config.type;
  }

  getPropName() {
    return this._config.propName;
  }

  getRenderer() {
    return this._renderer;
  }

  foundInFunctionalConfig() {
    return this._foundInFunctionalConfig;
  }

  setFoundInFunctionalConfig(value) {
    this._foundInFunctionalConfig = value;
  }

  hasName(propName) {
    return this.getFields().includes(propName);
  }

  getFields() {
    return this._config.propName || [];
  }

  addInitValues(initValues) {
    if (this._config.value === undefined) return initValues;

    return FormUtils.addInitValue(initValues, this._config.propName, this._config.value, this._config.multiplier);
  }

  addValidators(validations) {
    return FormUtils.addValidators(validations, this._config);
  }

}
