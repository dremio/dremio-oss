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

import BarChart from './BarChart';

describe('BarChart', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
    };
    commonProps = {
      data: [{
        x: 2,
        y: 1,
        range: { lowerLimit: 1, upperLimit: 3 }
      }]
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<BarChart {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render tooltip if openTooltip is true', () => {
    const wrapper = shallow(<BarChart {...commonProps} />);
    wrapper.setState({ hoverBarTooltipInfo: { position: {}, index: 0 } });
    expect(wrapper.find('ChartTooltip')).to.have.length(1);
  });

  describe('formatNumber', () => {
    it('should round number to 5 chars after comma for FLOAT', () => {
      const props = {
        ...commonProps,
        type: 'FLOAT'
      };
      const number = 3.12312322;
      const wrapper = shallow(<BarChart {...props} />);
      const instance = wrapper.instance();
      expect(instance.formatNumber(number)).to.eql('3.12312');
    });
  });

  describe('getTickValues', () => {
    it('should return evenly spaced indexes based on length', () => {
      const length = 10;
      const wrapper = shallow(<BarChart {...commonProps} />);
      const instance = wrapper.instance();
      const tickValues = instance.getTickValues(length);
      expect(tickValues).to.have.length(8);
      const firstDiff = tickValues[1] - tickValues[0];
      const secondDiff = tickValues[2] - tickValues[1];
      expect(firstDiff === secondDiff).to.eql(true);
    });
    it('should return tick values with length less than max tick count if length is less than max tick count', () => {
      const length = 7;
      const wrapper = shallow(<BarChart {...commonProps} />);
      const instance = wrapper.instance();
      expect(instance.getTickValues(length)).to.have.length(6);
    });
  });
});
