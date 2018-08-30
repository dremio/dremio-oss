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
import { Component } from 'react';

import Radio from 'components/Fields/Radio';

import NewFieldSection from 'components/Forms/NewFieldSection';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { horizontalContentPadding } from '@app/uiTheme/less/layout.less';
import TransformForm, {formWrapperProps} from '../../forms/TransformForm';
import { title, description, radioStacked, newField } from './ConvertFloatToIntForm.less';
import { transformProps } from './../../forms/TransformationPropTypes';

const SECTIONS = [NewFieldSection];

export class ConvertFloatToIntForm extends Component {
  static propTypes = transformProps;

  constructor(props) {
    super(props);
  }

  render() {
    const {fields, submit} = this.props;
    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={submit}>
        <div clasName={horizontalContentPadding}>
          <span className={title}>{la('Rounding:')}</span>
          <Radio {...fields.rounding} label='Floor' radioValue='FLOOR' className={radioStacked} />
          <span className={description}>
            {la('Returns the largest integer less than or equal to the value.')}
          </span>
          <Radio {...fields.rounding} label='Ceiling' radioValue='CEILING' className={radioStacked} />
          <span className={description}>
            {la('Returns the smallest integer greater than or equal to the value.')}
          </span>
          <Radio {...fields.rounding} label='Round' radioValue='ROUND' className={radioStacked} />
          <div className={description}>
            {la('Returns the closest integer to the value.')}
          </div>
          <NewFieldSection fields={fields} className={newField} />
        </div>
      </TransformForm>
    );
  }
}

function mapStateToProps(state, props) {
  const { rounding, dropSourceField } = props.initialValues;
  return {
    initialValues: {
      type: 'ConvertFloatToInteger',
      rounding: rounding || 'FLOOR',
      newFieldName: props.columnName,
      dropSourceField: dropSourceField !== undefined ? dropSourceField : true
    }
  };
}

export default connectComplexForm({
  form: 'convertToInteger',
  fields: ['type', 'rounding'],
  overwriteOnInitialValuesChange: false
}, SECTIONS, mapStateToProps, null)(ConvertFloatToIntForm);
