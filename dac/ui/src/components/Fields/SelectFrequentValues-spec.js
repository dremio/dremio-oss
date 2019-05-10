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
import { shallow } from 'enzyme';
import ExploreCellLargeOverlay from '@app/pages/ExplorePage/components/ExploreTable/ExploreCellLargeOverlay';
import SelectFrequentValues from './SelectFrequentValues';

describe('SelectFrequentValues', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      field: {
        value: [],
        onChange: sinon.spy((obj) => minimalProps.field.value = obj)
      }
    };
    commonProps = {
      ...minimalProps,
      options: [
        { percent: 25, value: 'foo' },
        { percent: 25, value: 'bar' },
        { percent: 25, value: 'baz' },
        { percent: 25, value: '' }
      ]
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SelectFrequentValues {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render SelectFrequentValuesOption component for each props.options', () => {
    const wrapper = shallow(<SelectFrequentValues {...commonProps}/>);
    expect(wrapper.find('SelectFrequentValuesOption')).to.have.length(commonProps.options.length);
  });
  it('should render ExploreCellLargeOverlay if activeCell is defined', () => {
    const wrapper = shallow(<SelectFrequentValues {...commonProps}/>);
    wrapper.setState({
      activeCell: { cellValue: 'foo', anchor: document.createElement('span') }
    });
    expect(wrapper.find(ExploreCellLargeOverlay)).to.have.length(1);
  });
  describe('#handleCheck', () => {
    it('should return call onChange with checked values', () => {
      const instance = shallow(<SelectFrequentValues {...commonProps}/>).instance();
      instance.handleCheck('foo', true);
      expect(commonProps.field.onChange).to.be.calledWith(['foo']);
      instance.handleCheck('foo', false);
      expect(commonProps.field.onChange).to.be.calledWith([]);
    });
  });
});
