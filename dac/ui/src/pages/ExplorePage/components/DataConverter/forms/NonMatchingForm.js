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
import Radium from 'radium';

import { Radio } from 'components/Fields';
import NewFieldSection from 'components/Forms/NewFieldSection';
import { FieldWithError, TextField } from 'components/Fields';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import { formSectionTitle } from 'uiTheme/radium/exploreTransform';
import { FLEX_COL_START, LINE_ROW_START_CENTER } from 'uiTheme/radium/flexStyle';

import { isRequired, applyValidators } from 'utils/validation';
import TransformForm, { formWrapperProps } from '../../forms/TransformForm';
import { transformProps } from './../../forms/TransformationPropTypes';

const SECTIONS = [NewFieldSection];

function validate(values) {
  if (values.actionForNonMatchingValue === 'REPLACE_WITH_DEFAULT') {
    return applyValidators(values, [isRequired('defaultValue', 'Value')]);
  }
}

@Radium
export class NonMatchingForm extends Component {
  static propTypes = transformProps;

  constructor(props) {
    super(props);
    this.submit = this.submit.bind(this);
  }

  submit(form, submitType) {
    const { actionForNonMatchingValue, defaultValue, ...rest } = form;
    const value = actionForNonMatchingValue === 'REPLACE_WITH_DEFAULT' ? { defaultValue } : null;
    const data =  { ...rest, ...value, actionForNonMatchingValue };
    return this.props.submit(data, submitType);
  }

  render() {
    const { fields } = this.props;
    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={this.submit}>
        <div style={styles.radioOption}>
          <span style={[formSectionTitle, {marginBottom: 5}]}>{la('Action for Non-matching Values')}</span>
          <Radio
            {...fields.actionForNonMatchingValue}
            style={styles.radio}
            label='Replace values with null'
            radioValue='REPLACE_WITH_NULL'/>
          <div style={LINE_ROW_START_CENTER}>
            <Radio
              {...fields.actionForNonMatchingValue}
              style={styles.radio}
              label='Replace values with:'
              radioValue='REPLACE_WITH_DEFAULT'/>
            <FieldWithError errorPlacement='right' {...fields.defaultValue} style={{marginLeft: 10}}>
              <TextField
                disabled={fields.actionForNonMatchingValue.value !== 'REPLACE_WITH_DEFAULT'}
                {...fields.defaultValue}
                style={{ width: 300 }}/>
            </FieldWithError>
          </div>
          <Radio
            {...fields.actionForNonMatchingValue}
            style={styles.radio}
            label='Delete records'
            radioValue='DELETE_RECORDS'/>
        </div>
        <NewFieldSection fields={fields}/>
      </TransformForm>
    );
  }
}

function mapStateToProps(state, props) {
  const { actionForNonMatchingValue, dropSourceField, defaultValue } = props.initialValues;

  return {
    initialValues: {
      desiredType: (props.toType || '').toUpperCase(),
      actionForNonMatchingValue: actionForNonMatchingValue || 'REPLACE_WITH_NULL',
      defaultValue: defaultValue || '',
      newFieldName: props.columnName,
      dropSourceField: dropSourceField !== undefined ? dropSourceField : true
    }
  };
}

export default connectComplexForm({
  form: 'actionNonMatching',
  fields: ['defaultValue', 'actionForNonMatchingValue', 'desiredType'],
  overwriteOnInitialValuesChange: false,
  validate
}, SECTIONS, mapStateToProps, null)(NonMatchingForm);

const styles = {
  radioOption: {
    margin: '10px 0 10px 10px',
    ...FLEX_COL_START
  },
  radio: {
    margin: '5px 0 5px 0',
    paddingLeft: 0
  }
};
