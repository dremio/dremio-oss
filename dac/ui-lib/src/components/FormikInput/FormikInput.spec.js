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

import FormValidationMessage from '../FormValidationMessage';
import Label from '../Label';

import FormikInput from './FormikInput';

const mockOnChange = jest.fn();

const defaultProps = {
  label: 'Sample Label',
  classes: {
    root: 'sample-root-class',
    input: 'sample-input-class',
    label: 'sample-label-class'
  },
  field: {
    name: 'sample-input-name',
    onChange: mockOnChange
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
    expect(wrapper.find('div.input-root').exists()).toBe(true);
    expect(wrapper.find(Label).exists()).toBe(true);
    expect(wrapper.find(Label).props().value).toEqual(defaultProps.label);
    expect(wrapper.find('input').exists()).toBe(true);
    expect(wrapper.find('input').props().name).toEqual(defaultProps.field.name);
  });

  it('adds the classes passed as props to respective elements', () => {
    expect(wrapper.find('div.input-root').props().className).toEqual(expect.stringContaining('sample-root-class'));
    expect(wrapper.find(Label).props().className).toEqual(expect.stringContaining('sample-label-class'));
    expect(wrapper.find('input').props().className).toEqual(expect.stringContaining('sample-input-class'));
  });

  it('triggers onChange sent in props', () => {
    const mockEvent = { key: 'sample val' };
    wrapper.find('input').at(0).simulate('change', mockEvent);
    expect(mockOnChange).toBeCalledTimes(1);
    expect(mockOnChange).toBeCalledWith(mockEvent);
  });


  it('display error message when field is touched and has error', () => {
    const errorProps = {
      ...defaultProps,
      form: {
        touched: {
          [defaultProps.field.name]: true
        },
        errors: {
          [defaultProps.field.name]: 'Required'
        }
      }
    };
    const errorWrapper = getShallowWrapper(errorProps);
    expect(errorWrapper.find(FormValidationMessage).exists()).toBe(true);
    expect(errorWrapper.find(FormValidationMessage).props().children).toBe('Required');
  });

  it('handles error message for nested fields', () => {
    const errorProps = {
      ...defaultProps,
      field: {
        ...defaultProps.field,
        name: 'a.b.c'
      },
      form: {
        touched: {
          a: {
            b: {
              c: true
            }
          }
        },
        errors: {
          a: {
            b: {
              c: 'Required'
            }
          }
        }
      }
    };
    const errorWrapper = getShallowWrapper(errorProps);
    expect(errorWrapper.find(FormValidationMessage).exists()).toBe(true);
    expect(errorWrapper.find(FormValidationMessage).props().children).toBe('Required');
  });

  it('shows error message only when the field is touched', () => {
    const errorProps = {
      ...defaultProps,
      form: {
        touched: {
        },
        errors: {
          [defaultProps.field.name]: 'Required'
        }
      }
    };
    const errorWrapper = getShallowWrapper(errorProps);
    expect(errorWrapper.find(FormValidationMessage).exists()).toBe(false);
  });

  it('hides error message when hideError is true', () => {
    const errorProps = {
      ...defaultProps,
      form: {
        touched: {
          [defaultProps.field.name]: true
        },
        errors: {
          [defaultProps.field.name]: 'Required'
        }
      },
      hideError: true
    };
    const errorWrapper = getShallowWrapper(errorProps);
    expect(errorWrapper.find(FormValidationMessage).exists()).toBe(false);
  });

});
