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
import HostList from 'components/Forms/HostList';
import HostListWrapper from 'components/Forms/Wrappers/HostListWrapper';

export default class HostListConfig extends FormElementConfig {

  constructor(props) {
    super(props);
    this._renderer = HostListWrapper;
  }

  getRenderer() {
    return this._renderer;
  }

  getFields() {
    return HostList.getFields(super.getConfig());
  }

  addInitValues(initValues, state, props) {
    const elementConfig = super.getConfig();
    const {editing} = props;
    if (editing) {
      FormUtils.addInitValueForEditing(initValues, super.getPropName(), state);
    } else if (elementConfig.showOneRowForEmptyList !== false) {
      initValues = FormUtils.addInitValue(initValues, elementConfig.propName, [{port: elementConfig.defaultPort}]);
    }
    return initValues;
  }

  addValidators(validations) {
    validations.functions.push(HostList.getValidators(super.getConfig()));
    return validations;
  }

}
