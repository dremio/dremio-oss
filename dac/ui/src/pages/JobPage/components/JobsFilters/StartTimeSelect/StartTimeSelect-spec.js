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
import moment from 'moment';
import * as IntervalTypes from './IntervalTypes';
import StartTimeSelect from './StartTimeSelect';

describe('StartTimeSelect', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      startTime: 0,
      endTime: 0,
      onChange: sinon.spy(),
      id: '1'
    };
    commonProps = {
      ...minimalProps,
      defaultType: IntervalTypes.ALL_TIME_INTERVAL
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<StartTimeSelect {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render with common props without exploding', () => {
    const wrapper = shallow(<StartTimeSelect {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });
  describe('getSelectedInterval', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<StartTimeSelect {...commonProps}/>);
      instance = wrapper.instance();
    });
    it('should return correct interval based on chosen range', () => {
      let startTime = Number(moment().subtract(1, 'h').valueOf());
      let endTime = Number(moment().valueOf());
      wrapper.setProps({
        startTime,
        endTime
      });
      expect(instance.getSelectedInterval().get('type')).to.eql(IntervalTypes.LAST_HOUR_INTERVAL);

      startTime = Number(moment().subtract(6, 'h'));
      endTime = Number(moment());
      wrapper.setProps({
        startTime,
        endTime
      });
      expect(instance.getSelectedInterval().get('type')).to.eql(IntervalTypes.LAST_6_HOURS_INTERVAL);
    });
    it('should return undefined if range is not match with other intervals', () => {
      const startTime = Number(moment().subtract(2, 'h'));
      const endTime = Number(moment());
      wrapper.setProps({
        startTime,
        endTime
      });
      expect(instance.getSelectedInterval()).to.be.undefined;
    });
    it('should return defaultType if range is empty', () => {
      expect(instance.getSelectedInterval().get('type')).to.eql(IntervalTypes.ALL_TIME_INTERVAL);
    });
  });
});
