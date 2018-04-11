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

import DurationInput from './DurationInput';

const minute = 60 * 1000;
const hour = 60 * minute;
const day = 24 * hour;
const week = 7 * day;

describe('DurationInput', () => {
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      fields: {
        duration: 1000
      }
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DurationInput {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#constructFields', () => {
    it('should convert 15 minutes in milliseconds to field value', () => {
      expect(DurationInput.constructFields(15 * minute)).to.be.eql({
        duration: 15,
        unit: 'MINUTES'
      });
    });

    it('should convert 22 hours in milliseconds to field value', () => {
      expect(DurationInput.constructFields(22 * hour)).to.be.eql({
        duration: 22,
        unit: 'HOURS'
      });
    });

    it('should convert 3 days in milliseconds to field value', () => {
      expect(DurationInput.constructFields(3 * day)).to.be.eql({
        duration: 3,
        unit: 'DAYS'
      });
    });

    it('should convert 5 weeks in milliseconds to field value', () => {
      expect(DurationInput.constructFields(5 * week)).to.be.eql({
        duration: 5,
        unit: 'WEEKS'
      });
    });
  });

  describe('#convertToMilliseconds', () => {
    it('should convert 15 minutes to milliseconds', () => {
      expect(DurationInput.convertToMilliseconds({
        duration: 15,
        unit: 'MINUTES'
      })).to.be.eql(15 * minute);
    });

    it('should convert 60 minutes to 1 hour', () => {
      expect(DurationInput.convertToMilliseconds({
        duration: 1,
        unit: 'HOURS'
      })).to.be.eql(60 * minute);
    });

    it('should convert 22 hours to milliseconds', () => {
      expect(DurationInput.convertToMilliseconds({
        duration: 22,
        unit: 'HOURS'
      })).to.be.eql(22 * hour);
    });

    it('should convert 24 hours to 1 day', () => {
      expect(DurationInput.convertToMilliseconds({
        duration: 1,
        unit: 'DAYS'
      })).to.be.eql(24 * hour);
    });

    it('should convert 3 days to milliseconds', () => {
      expect(DurationInput.convertToMilliseconds({
        duration: 3,
        unit: 'DAYS'
      })).to.be.eql(3 * day);
    });

    it('should convert 7 days to 1 week', () => {
      expect(DurationInput.convertToMilliseconds({
        duration: 1,
        unit: 'WEEKS'
      })).to.be.eql(7 * day);
    });

    it('should convert 5 weeks to milliseconds', () => {
      expect(DurationInput.convertToMilliseconds({
        duration: 5,
        unit: 'WEEKS'
      })).to.be.eql(5 * week);
    });
  });
});
