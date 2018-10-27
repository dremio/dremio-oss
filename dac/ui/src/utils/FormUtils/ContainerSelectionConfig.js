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
import SourceFormJsonPolicy from 'utils/FormUtils/SourceFormJsonPolicy';
import FormSectionConfig from 'utils/FormUtils/FormSectionConfig';
import ContainerSelectionWrapper from 'components/Forms/Wrappers/ContainerSelectionWrapper';

export default class ContainerSelectionConfig extends FormElementConfig {

  constructor(config, functionalElements) {
    super(config);
    this._renderer = ContainerSelectionWrapper;

    if (!config.options) return;

    config.options = config.options.map(option => {
      if (option.container && option.container.propName) {
        // in case container is just one element
        option.container = SourceFormJsonPolicy.joinConfigsAndConvertElementToObj(option.container, functionalElements);
      } else if (option.container) {
        // container is a section
        option.container = new FormSectionConfig(option.container, functionalElements);
      } else {
        // container is not defined - error in config - skip option
        return null;
      }
      return option;
    }).filter(Boolean);
  }

  getRenderer() {
    return this._renderer;
  }

  getFields() {
    // selector element propName and all field names from option containers
    return [this.getPropName()]
      .concat(this.getOptions().reduce((accum, option) => {
        return accum.concat(option.container.getFields());
      }, []));
  }

  getOptions() {
    return super.getConfig().options;
  }

  addInitValues(initValues, state, props) {
    const elementConfig = super.getConfig();
    initValues = FormUtils.addInitValue(initValues, elementConfig.propName, elementConfig.value);
    for (const option of elementConfig.options) {
      initValues = option.container.addInitValues(initValues, state, props);
    }
    return initValues;
  }

  addValidators(validations) {
    const elementConfig = super.getConfig();
    if (elementConfig.options) {
      for (const option of elementConfig.options) {
        if (option.container) {
          validations = option.container.addValidators(validations);
        }
      }
    }
    return validations;
  }

}
