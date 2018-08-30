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
import SourceFormJsonPolicy from 'utils/FormUtils/SourceFormJsonPolicy';
import FormSectionConfig from 'utils/FormUtils/FormSectionConfig';
import FormUtils from 'utils/FormUtils/FormUtils';

export default class FormTabConfig {
  constructor(configJson, functionalElements) {
    this._config = FormUtils.deepCopyConfig(configJson) || {};

    if (this._config.sections) {
      this._config.sections = this._config.sections.map(
        section => new FormSectionConfig(section, functionalElements));
    }
    if (this._config.elements) {
      this._config.elements = this._config.elements.map(
        element => SourceFormJsonPolicy.joinConfigsAndConvertElementToObj(element, functionalElements));
    }
  }

  getConfig() {
    return this._config;
  }

  getName() {
    return this._config.name || '';
  }

  getTitle(formConfig) {
    if (this.isGeneral()) {
      return `${formConfig.label} Source`;
    }
    if (this._config.hideTitle) {
      return null;
    }
    if (this._config.title) {
      return this._config.title;
    }
    return this._config.name || '';
  }

  isGeneral() {
    return this._config.isGeneral;
  }

  getSections() {
    return this._config.sections || [];
  }

  getFields() {
    return this.getDirectElements().reduce((fields, element) => fields.concat(element.getFields()), [])
      .concat(this.getSections().reduce((fields, section) => fields.concat(section.getFields()), []));
  }

  addInitValues(initValues, state, props) {
    initValues = this.getDirectElements().reduce((accum, element) => element.addInitValues(accum, state, props), initValues);
    return this.getSections().reduce((accum, section) => section.addInitValues(accum, state, props), initValues);
  }

  addValidators(validations) {
    validations = this.getDirectElements().reduce((accum, element) => element.addValidators(accum), validations);
    return this.getSections().reduce((accum, section) => section.addValidators(accum), validations);
  }

  addSection(sectionConfig, position = 'tail') {
    this._config.sections = this._config.sections || [];
    if (position === 'head') {
      this._config.sections.unshift(sectionConfig);
    } else {
      this._config.sections.push(sectionConfig);
    }
  }

  getDirectElements() {
    return this._config.elements || [];
  }

  getAllElements() {
    let elements = this.getDirectElements();
    this.getSections().forEach(section => {
      elements = elements.concat(section.getAllElements());
    });
    return elements;
  }

  removeNotFoundElements() {
    if (this.getDirectElements().length) {
      this._config.elements = this.getDirectElements().filter(element => element.foundInFunctionalConfig());
    }
    this.getSections().forEach(section => section.removeNotFoundElements());
  }

}

