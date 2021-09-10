/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import React from 'react';
import { shallow } from 'enzyme';

import Input from '../Input';

import FormikInput from './FormikInput';

const mockOnChange = jest.fn();
const mockOnBlur = jest.fn();

const defaultProps = {
  label: 'Sample Label',
  classes: {
    root: 'sample-root-class',
    input: 'sample-input-class',
    label: 'sample-label-class'
  },
  field: {
    name: 'sample-input-name',
    onChange: mockOnChange,
    onBlur: mockOnBlur
  },
  form: {
    errors: {},
    touched: {}
  }
};

const getShallowWrapper = (props = defaultProps) => {
  return shallow(<FormikInput {...props}/>);
};

describe('Formik Input', () => {
  const wrapper = getShallowWrapper();
  it('has the required components', () => {
    expect(wrapper.find(Input).exists()).toBe(true);
    expect(wrapper.find(Input).props().name).toEqual(defaultProps.field.name);
    expect(wrapper.find(Input).props().onChange).toEqual(mockOnChange);
    expect(wrapper.find(Input).props().onBlur).toEqual(mockOnBlur);
    expect(wrapper.find(Input).props().form).toEqual(defaultProps.form);
    expect(wrapper.find(Input).props().label).toEqual(defaultProps.label);
  });
});
