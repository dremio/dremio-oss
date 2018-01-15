/*
 * Copyright (C) 2017 Dremio Corporation
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

import Radio from 'components/Fields/Radio';

import NewFieldSection from 'components/Forms/NewFieldSection';
import { connectComplexForm } from 'components/Forms/connectComplexForm';

import { formLabel, formDescription } from 'uiTheme/radium/typography';
import { FLEX_NOWRAP_COL_START } from 'uiTheme/radium/flexStyle';

import TransformForm, {formWrapperProps} from '../../forms/TransformForm';
import { transformProps } from './../../forms/TransformationPropTypes';

const SECTIONS = [NewFieldSection];

@Radium
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
        <div style={styles.wrap}>
          <span style={formLabel}>{la('Rounding:')}</span>
          <Radio {...fields.rounding} style={styles.radio} label='Floor' radioValue='FLOOR'/>
          <span style={styles.text}>
            {la('Returns the largest integer less than or equal to the value.')}
          </span>
          <Radio {...fields.rounding} style={styles.radio} label='Ceiling' radioValue='CEILING'/>
          <span style={styles.text}>
            {la('Returns the smallest integer greater than or equal to the value.')}
          </span>
          <Radio {...fields.rounding} style={styles.radio} label='Round' radioValue='ROUND'/>
          <div style={styles.text}>
            {la('Returns the closest integer to the value.')}
          </div>
        </div>
        <NewFieldSection fields={fields}/>
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

const styles = {
  wrap: {
    ...FLEX_NOWRAP_COL_START,
    margin: '10px 0 10px 10px'
  },
  radio: {
    display: 'inline-flex',
    alignItems: 'center',
    marginTop: 5
  },
  text: {
    ...formDescription,
    marginLeft: 18
  }
};
