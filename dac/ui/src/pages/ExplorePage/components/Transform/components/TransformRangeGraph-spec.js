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
import TransformRangeGraph from './TransformRangeGraph';
describe('TransformRangeGraph', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      updateValue: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      columnType: 'DATETIME',
      width: 100,
      leftRangeOffset: 0,
      rightRangeOffset: 10
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<TransformRangeGraph {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render with common props without exploding', () => {
    const wrapper = shallow(<TransformRangeGraph {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#resetRangeOffsets', () => {
    it('should set state with default offsets if indexes is undefined', () => {
      const props = {
        ...minimalProps,
        width: 100
      };
      const wrapper = shallow(<TransformRangeGraph {...commonProps}/>);
      const instance = wrapper.instance();
      instance.resetRangeOffsets(props);
      expect(wrapper.state('leftRangeOffset')).to.equal(0);
      expect(wrapper.state('rightRangeOffset')).to.equal(props.width);
    });
    it('should set offsets based on offset indexes', () => {
      const props = {
        leftRangeOffset: 40,
        rightRangeOffset: 60
      };
      const wrapper = shallow(<TransformRangeGraph {...commonProps}/>);
      const instance = wrapper.instance();
      instance.resetRangeOffsets(props);
      expect(wrapper.state('leftRangeOffset')).to.equal(42);
      expect(wrapper.state('rightRangeOffset')).to.equal(60);
    });
  });
});
