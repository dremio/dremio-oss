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
import FormElementConfig from 'utils/FormUtils/FormElementConfig';
import DataFreshnessSection from 'components/Forms/DataFreshnessSection';
import DataFreshnessWrapper from 'components/Forms/Wrappers/DataFreshnessWrapper';

export default class DataFreshnessConfig extends FormElementConfig {

  constructor(props) {
    super(props);
    this._renderer = DataFreshnessWrapper;
  }

  getRenderer() {
    return this._renderer;
  }

  getFields() {
    return DataFreshnessSection.getFields();
  }

  addInitValues(initValues) {
    initValues.accelerationRefreshPeriod = DataFreshnessSection.defaultFormValueRefreshInterval();
    initValues.accelerationGracePeriod = DataFreshnessSection.defaultFormValueGracePeriod();
    return initValues;
  }

  addValidators(validations) {
    validations.functions.push(DataFreshnessSection.validate);
    return validations;
  }

}
