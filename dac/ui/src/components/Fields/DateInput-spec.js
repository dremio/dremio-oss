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
import moment from 'moment';
import { shallow } from 'enzyme';
import { DATE, TIME, dateTypeToFormat, DATETIME } from 'constants/DataTypes';

import DateInput from './DateInput';
const getDD = wrapper => shallow(wrapper.find('SelectView').prop('children')({}));

describe('DateInput', () => {
  beforeEach(() => {
    sinon.stub(moment, 'now').returns(Date.now()); // lock in the date to avoid cross-millisecond tests
  });

  afterEach(() => {
    moment.now.restore();
  });

  describe('#mergeDateWithTime', () => {
    it('should merge date with time correctly', () => {
      const dateTest = '2010-12-30';
      const timeTest = '12-13-23.123';
      const result = '2010-12-30 12:13:23';
      const dateMoment = moment(dateTest, dateTypeToFormat[DATE]);
      const timeMoment = moment(timeTest, dateTypeToFormat[TIME]);
      const dateMomentPre = dateMoment.toString();
      const timeMomentPre = timeMoment.toString();
      expect(DateInput.mergeDateWithTime(dateMoment, timeMoment, DATETIME)).to.be.equal(result);

      // don't mutate
      expect(dateMoment.toString()).to.equal(dateMomentPre);
      expect(timeMoment.toString()).to.equal(timeMomentPre);
    });

    it('should merge date with time correctly, even at the end of a month leading up to a month with fewer days', () => {
      moment.now.returns(Date.parse('Feb 1, 2017 12:51:56 AM')); // tests actually found bug on this day/time

      const dateTest = '2010-12-30';
      const timeTest = '12-13-23.123';
      const result = '2010-12-30 12:13:23';
      const dateMoment = moment(dateTest, dateTypeToFormat[DATE]);
      const timeMoment = moment(timeTest, dateTypeToFormat[TIME]);
      expect(DateInput.mergeDateWithTime(dateMoment, timeMoment, DATETIME)).to.be.eql(result);
    });
  });

  describe('#handleTimeChange', () => {
    let wrapper;
    const props = { onChange: sinon.spy(), type: TIME };
    beforeEach(() => {
      wrapper = shallow(<DateInput {...props}/>);
      sinon.spy(DateInput, 'mergeDateWithTime');
    });
    afterEach(() => {
      DateInput.mergeDateWithTime.restore();
    });

    it('should call DateInput.mergeDateWithTime with dateMoment like current day if !props.value', () => {
      const instance = wrapper.instance();
      const now = moment().startOf('hour');
      const curDay = moment().startOf('day');

      instance.handleTimeChange(now);

      expect(DateInput.mergeDateWithTime(curDay, now, props.type));
    });

    it('should call DateInput.mergeDateWithTime with dateMoment like props.value', () => {
      wrapper.setProps({ type: DATE, value: '1970-01-01' });
      const instance = wrapper.instance();
      const now = moment().startOf('hour');
      const dateMoment = moment(props.value, dateTypeToFormat[DATE]);

      instance.handleTimeChange(now);

      expect(DateInput.mergeDateWithTime(dateMoment, now, props.type));
    });
  });

  describe('#renderCalendar', () => {
    it('should render TimePicker with props.value', () => {
      const props = {
        type: TIME,
        onChange: sinon.spy(),
        value: '00:00:00'
      };
      const wrapper = shallow(<DateInput {...props}/>);

      expect(getDD(wrapper).find('TimePicker').prop('value')).to.eql(props.value);
    });

    it('should render TimePicker with moment().format(hh:mm:ss) if !props.value', () => {
      const props = {
        type: TIME,
        onChange: sinon.spy()
      };
      const wrapper = shallow(<DateInput {...props}/>);

      expect(getDD(wrapper).find('TimePicker').prop('value')).to.eql(moment().format('hh:mm:ss'));
    });

    it('should render DateRange with props.value', () => {
      const props = {
        type: DATE,
        onChange: sinon.spy(),
        value: '1970-01-01'
      };
      const wrapper = shallow(<DateInput {...props}/>);

      expect(getDD(wrapper).find('DateRange').prop('startDate')).to.eql(moment(props.value));
      expect(getDD(wrapper).find('DateRange').prop('endDate')).to.eql(moment(props.value));
    });

    it('should render DateRange with current day if !props.value', () => {
      const props = {
        type: DATE,
        onChange: sinon.spy()
      };
      const wrapper = shallow(<DateInput {...props}/>);
      const curDay = moment().startOf('day');

      expect(getDD(wrapper).find('DateRange').prop('startDate')).to.eql(curDay);
      expect(getDD(wrapper).find('DateRange').prop('endDate')).to.eql(curDay);
    });
  });

  describe('#handleCalendarSelect', () => {
    let props;
    let wrapper;
    let instance;
    let closeDD;
    beforeEach(() => {
      closeDD = sinon.spy();
      props = {
        type: DATE,
        onChange: sinon.spy(),
        value: '1970-01-01'
      };
      wrapper = shallow(<DateInput {...props}/>);
      instance = wrapper.instance();
      wrapper.setState({
        isOpen: true
      });
    });
    it('should change isOpen to false, call onChange if new value is different', () => {
      const newValue = '2010-12-30';
      instance.handleCalendarSelect(closeDD, {
        startDate: moment(newValue, dateTypeToFormat[DATE])
      });

      expect(props.onChange).to.be.called;
      expect(closeDD).to.be.called;
    });
    it('should not call onChange if new value is the same', () => {
      const newValue = '1970-01-01';
      instance.handleCalendarSelect(closeDD, {
        startDate: moment(newValue, dateTypeToFormat[DATE])
      });

      expect(props.onChange).to.be.not.called;
      expect(wrapper.state('isOpen')).to.be.true;
    });
  });
});
