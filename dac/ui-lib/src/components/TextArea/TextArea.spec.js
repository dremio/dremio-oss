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

import TextareaAutosize from '@material-ui/core/TextareaAutosize';

import Label from '../Label';
import CopyToClipboard from '../CopyToClipboard';

import TextArea from './TextArea';

const minimalProps = {
  classes: {
    root: 'sample-root-class',
    label: 'sample-label-class',
    container: 'sample-container-class'
  },
  value: 'Test Content',
  label: 'Test Label'
};

const allProps = {
  ...minimalProps,
  noResize: true,
  maxLines: 4,
  disabled: true,
  enableCopy: true
};

const getShallowWrapper = (props = minimalProps) => {
  return shallow(<TextArea {...props} />);
};

describe('TextArea', () => {
  it('renders required components with minimal props', () => {
    const wrapper = getShallowWrapper();
    expect(wrapper.find('.textAreaRoot').exists()).toBe(true);
    expect(wrapper.find(Label).exists()).toBe(true);
    expect(wrapper.find(Label).props().value).toEqual(minimalProps.label);
    expect(wrapper.find(TextareaAutosize).exists()).toBe(true);
    expect(wrapper.find(TextareaAutosize).props()).toEqual(expect.objectContaining({
      value: minimalProps.value,
      disabled: false, //Default
      className: '' // no-resize is false by default
    }));
    expect(wrapper.find(CopyToClipboard).exists()).toBe(false);
  });

  it('renders required components when all props are provided', () => {
    const wrapper = getShallowWrapper(allProps);
    expect(wrapper.find('.textAreaRoot').exists()).toBe(true);
    expect(wrapper.find(Label).exists()).toBe(true);
    expect(wrapper.find(Label).props().value).toEqual(minimalProps.label);
    expect(wrapper.find(TextareaAutosize).exists()).toBe(true);
    expect(wrapper.find(TextareaAutosize).props()).toEqual(expect.objectContaining({
      value: minimalProps.value,
      disabled: true,
      className: '--noResize',
      rowsMax: 4
    }));
    expect(wrapper.find(CopyToClipboard).exists()).toBe(true);
  });

  it('adds the classes passed as props to respective elements', () => {
    const wrapper = getShallowWrapper();
    expect(wrapper.find('div.textAreaRoot').props().className).toEqual(expect.stringContaining('sample-root-class'));
    expect(wrapper.find(Label).props().className).toEqual(expect.stringContaining('sample-label-class'));
    expect(wrapper.find('div.textAreaRoot__container').props().className).toEqual(expect.stringContaining('sample-container-class'));
  });
});
