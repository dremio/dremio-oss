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
import NewFieldSection from 'components/Forms/NewFieldSection';
import { connectComplexForm } from 'components/Forms/connectComplexForm';

import { isRequired, applyValidators } from 'utils/validation';
import { TextToDateForm } from './TextToDateForm';

// TODO: look into creating a base class this and TextToDateForm can inherit from.

const SECTIONS = [NewFieldSection];

function validate(values) {
  if (values.format === 'CUSTOM') {
    return applyValidators(values, [isRequired('customValue', 'Custom')]);
  }
}

function mapStateToProps(state, props) {
  const { format, dropSourceField } = props.initialValues;
  const { fromType } = props;

  const formats = TextToDateForm.getFormats(fromType);

  let initialFormat = format || formats.values[0];
  initialFormat = formats.values.indexOf(initialFormat) !== -1 ? initialFormat : 'CUSTOM';

  const customValue = formats.values.indexOf(format) !== -1 ? '' : format;

  return {
    toType: fromType,
    hideNotMatchingOptions: true,
    initialValues: {
      format: initialFormat,
      customValue: customValue || '',
      newFieldName: props.columnName,
      dropSourceField: dropSourceField !== undefined ? dropSourceField : true
    }
  };
}

export default connectComplexForm({
  form: 'dateToTextForm',
  fields: ['customValue', 'format'],
  overwriteOnInitialValuesChange: false,
  validate
}, SECTIONS, mapStateToProps, null)(TextToDateForm);
