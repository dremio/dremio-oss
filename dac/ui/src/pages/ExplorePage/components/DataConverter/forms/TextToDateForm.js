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
import PropTypes from 'prop-types';

import Radio from 'components/Fields/Radio';
import { FieldWithError, TextField } from 'components/Fields';
import NewFieldSection from 'components/Forms/NewFieldSection';
import { connectComplexForm } from 'components/Forms/connectComplexForm';

import { formLabel } from 'uiTheme/radium/typography';

import { isRequired, applyValidators } from 'utils/validation';
import { sectionTitle, inputForRadio, radioStacked, rowMargin } from '@app/uiTheme/less/forms.less';
import TransformForm, {formWrapperProps} from '../../forms/TransformForm';
import { transformProps } from './../../forms/TransformationPropTypes';
import FORMATS from './DateFormatOptions';
import { base, docArea, newField } from './TextToDateForm.less';

const SECTIONS = [NewFieldSection];

function validate(values) {
  if (values.format === 'CUSTOM') {
    return applyValidators(values, [isRequired('customValue', 'Custom')]);
  }
}

export class TextToDateForm extends Component {
  static propTypes = {
    ...transformProps,
    hideNotMatchingOptions: PropTypes.bool
  };

  constructor(props) {
    super(props);
    this.submit = this.submit.bind(this);
  }

  submit(form, submitType) {
    const { format, customValue, toType, ...rest } = form;

    let desiredType = null;
    if (rest.type && rest.type.indexOf('ToDate') !== -1) {
      desiredType = { desiredType: toType };
    }

    const data =  { ...rest, ...desiredType,  format: format !== 'CUSTOM' ? format : customValue };
    return this.props.submit(data, submitType);
  }

  static getFormats(type) {
    return FORMATS[type];
  }

  render() {
    const { fields, toType, hideNotMatchingOptions } = this.props;

    const formats = TextToDateForm.getFormats(toType);

    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={this.submit}>
        <div className={base}>
          <div>
            <div style={formLabel}>{la('Format')}</div>
            {formats.values.map((format) => <Radio {...fields.format} label={format} radioValue={format} className={radioStacked} />)}
            <Radio {...fields.format} label={la('Custom:')} radioValue='CUSTOM' className={radioStacked} />
            <FieldWithError errorPlacement='right' {...fields.customValue} className={inputForRadio}>
              <TextField {...fields.customValue} disabled={fields.format.value !== 'CUSTOM'} style={{ width: 300 }}/>
            </FieldWithError>
            {!hideNotMatchingOptions && (<div>
              <div className={sectionTitle}>{la('Action for Non-matching Values')}</div>
              <div>
                <Radio {...fields.actionForNonMatchingValue} label={la('Replace values with null')}
                  radioValue='REPLACE_WITH_NULL'
                  className={radioStacked} />
                <Radio {...fields.actionForNonMatchingValue} label={la('Delete records')}
                  radioValue='DELETE_RECORDS'
                  className={radioStacked}/>
              </div>
            </div>)}
            <NewFieldSection fields={fields} className={newField}/>
          </div>
          <div className={docArea}>
            <div style={formLabel}>{la('Formatting Options')}</div>

            {formats.examples.map((example, index) =>
              <div key={index} className={rowMargin}>{example.format}: {example.description}</div>
            )}

            <div className={rowMargin}>
              <a href='https://docs.dremio.com/working-with-datasets/data-curation.html?q=#working-with-dates'
                target='_blank'>
                {la('Learn more…')}
              </a>
            </div>
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

