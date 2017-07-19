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
import { FieldWithError, TextField } from 'components/Fields';
import NewFieldSection from 'components/Forms/NewFieldSection';
import { connectComplexForm } from 'components/Forms/connectComplexForm';

import { formLabel, body } from 'uiTheme/radium/typography';
import { LINE_START_CENTER } from 'uiTheme/radium/flexStyle';

import { isRequired, applyValidators } from 'utils/validation';
import TransformForm, {formWrapperProps} from '../../forms/TransformForm';
import { transformProps } from './../../forms/TransformationPropTypes';
import FORMATS from './DateFormatOptions';

const SECTIONS = [NewFieldSection];

function validate(values) {
  if (values.format === 'CUSTOM') {
    return applyValidators(values, [isRequired('customValue', 'Custom')]);
  }
}

@Radium
export class TextToDateForm extends Component {
  static propTypes = transformProps;

  constructor(props) {
    super(props);
    this.submit = this.submit.bind(this);
  }

  submit(form, submitType) {
    const { format, customValue, toType, ...rest } = form;
    const desiredType = rest.type.indexOf('ToDate') !== -1 ? {desiredType: toType} : null;
    const data =  { ...rest, ...desiredType,  format: format !== 'CUSTOM' ? format : customValue };
    return this.props.submit(data, submitType);
  }

  static getFormats(type) {
    return FORMATS[type];
  }

  render() {
    const { fields, toType } = this.props;

    const formats = TextToDateForm.getFormats(toType);

    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={this.submit}>
        <div style={styles.base}>
          <div>
            <div style={formLabel}>{la('Format')}</div>
            {formats.values.map((format) =>
              <Radio {...fields.format} style={styles.row} label={format} radioValue={format}/>)}
            <div style={{...LINE_START_CENTER, ...styles.row}}>
              <Radio {...fields.format} label={la('Custom:')} radioValue='CUSTOM'/>
              <FieldWithError errorPlacement='right' {...fields.customValue} style={styles.input}>
                <TextField {...fields.customValue} disabled={fields.format.value !== 'CUSTOM'} style={{ width: 300 }}/>
              </FieldWithError>
            </div>
            <div style={{...formLabel, ...styles.row}}>{la('Action for Non-matching Values')}</div>
            <div>
              <Radio {...fields.actionForNonMatchingValue} style={styles.row} label={la('Replace values with null')}
                radioValue='REPLACE_WITH_NULL'/>
              <Radio {...fields.actionForNonMatchingValue} label={la('Delete records')} style={styles.row}
                radioValue='DELETE_RECORDS'/>
            </div>
            <NewFieldSection style={{...styles.row, marginLeft: 0}} fields={fields}/>
          </div>
          <div style={styles.docArea}>
            <div style={formLabel}>{la('Formatting Options')}</div>

            {formats.examples.map((example) =>
              <div style={styles.row}>{example.format}: {example.description}</div>
            )}

            <a href='https://docs.dremio.com/working-with-datasets/common-transformations.html?q=#working-with-dates'
              style={styles.row} target='_blank'>
              {la('Learn more…')}
            </a>
          </div>
        </div>
      </TransformForm>
    );
  }
}

function mapStateToProps(state, props) {
  const { format, dropSourceField } = props.initialValues;
  const { toType } = props;

  const formats = TextToDateForm.getFormats(toType);

  let initialFormat = format || formats.values[0];
  initialFormat = formats.values.indexOf(initialFormat) !== -1 ? initialFormat : 'CUSTOM';

  const customValue = formats.values.indexOf(format) !== -1 ? '' : format;

  return {
    initialValues: {
      type: 'ConvertTextToDate',
      toType: props.toType,
      format: initialFormat,
      actionForNonMatchingValue: 'REPLACE_WITH_NULL',
      customValue: customValue || '',
      newFieldName: props.columnName,
      dropSourceField: dropSourceField !== undefined ? dropSourceField : true
    }
  };
}

export default connectComplexForm({
  form: 'convertToDate',
  fields: ['type', 'customValue', 'format', 'toType', 'actionForNonMatchingValue'],
  overwriteOnInitialValuesChange: false,
  validate
}, SECTIONS, mapStateToProps, null)(TextToDateForm);

const styles = {
  base: {
    ...body,
    display: 'flex',
    flexDirection: 'row',
    margin: '10px 0 10px 10px'
  },
  input: {
    marginLeft: 10
  },
  row: {
    display: 'flex',
    alignItems: 'center',
    marginTop: 10
  },
  docArea: {
    flex: 1,
    marginLeft: 40
  }
};
