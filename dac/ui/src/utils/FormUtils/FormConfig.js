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
import FormTabConfig from 'utils/FormUtils/FormTabConfig';
import FormSectionConfig from 'utils/FormUtils/FormSectionConfig';

export default class FormConfig {
  constructor(formConfigJson, functionalElements) {
    this._config = formConfigJson || {};
    if (this._config.tabs) {
      this._config.tabs = this._config.tabs.map(
        tab => new FormTabConfig(tab, functionalElements));
    }
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

  getTabs() {
    return this._config.tabs || [];
  }

  getDirectSections() {
    return this._config.sections || [];
  }

  removeDirectSections() {
    delete this._config.sections;
  }

  getDirectElements() {
    return this._config.elements || [];
  }

  removeDirectElements() {
    delete this._config.elements;
  }

  getFields() {
    return this.getDirectElements().reduce((fields, element) => fields.concat(element.getFields()), [])
      .concat(this.getDirectSections().reduce((fields, section) => fields.concat(section.getFields()), []))
      .concat(this.getTabs().reduce((fields, tab) => fields.concat(tab.getFields()), []));
  }

  addInitValues(initValues, state, props) {
    initValues = this.getDirectElements().reduce((accum, element) => element.addInitValues(accum, state, props), initValues);
    initValues = this.getDirectSections().reduce((accum, section) => section.addInitValues(accum, state, props), initValues);
    return this.getTabs().reduce((accum, tab) => tab.addInitValues(accum, state, props), initValues);
  }

  addValidators(validations) {
    validations = this.getDirectElements().reduce((accum, element) => element.addValidators(accum), validations);
    validations = this.getDirectSections().reduce((accum, section) => section.addValidators(accum), validations);
    return this.getTabs().reduce((accum, tab) => tab.addValidators(accum), validations);
  }

  getAllElements() {
    let elements = this.getDirectElements();
    this.getDirectSections().forEach(section => {
      elements = elements.concat(section.getAllElements());
    });
    this.getTabs().forEach(tab => {
      elements = elements.concat(tab.getAllElements());
    });
    return elements;
  }

  addDirectElement(element) {
    this._config.elements = this._config.elements || [];
    this._config.elements.push(element);
  }

  findTabByName(tabName) {
    return this.getTabs().find(tab => tab.getName() === tabName);
  }

  getDefaultTab() {
    return this.getTabs().find(tab => tab.isGeneral()) || this.getTabs()[0];
  }

  removeNotFoundElements() {
    if (this.getDirectElements().length) {
      this._config.elements = this.getDirectElements().filter(element => element.foundInFunctionalConfig());
    }
    this.getDirectSections().forEach(section => section.removeNotFoundElements());
    this.getTabs().forEach(tab => tab.removeNotFoundElements());
  }

  addTab(tabConfig, position = 'tail') {
    this._config.tabs = this._config.tabs || [];
    if (position === 'head') {
      this._config.tabs.unshift(tabConfig);
    } else {
      this._config.tabs.push(tabConfig);
    }
  }
}
