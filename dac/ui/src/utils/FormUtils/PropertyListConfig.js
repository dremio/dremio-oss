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
import SourceProperties from 'components/Forms/SourceProperties';
import PropertyListWrapper from 'components/Forms/Wrappers/PropertyListWrapper';

export default class PropertyListConfig extends FormElementConfig {

  constructor(props) {
    super(props);
    this._renderer = PropertyListWrapper;
  }

  getRenderer() {
    return this._renderer;
  }

  getFields() {
    return SourceProperties.getFields(super.getConfig());
  }

  addInitValues(initValues, state, props) {
    const {editing} = props;
    if (editing) {
      FormUtils.addInitValueForEditing(initValues, super.getPropName(), state);
    }
    return initValues;
  }

  addValidators(validations) {
    validations.functions.push(SourceProperties.getValidators(super.getConfig()));
    return validations;
  }

}
