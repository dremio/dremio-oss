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
import { Component } from 'react';
import Immutable from 'immutable';
import moment from 'moment';
import PropTypes from 'prop-types';
import { SelectView } from '@app/components/Fields/SelectView';
import * as IntervalTypes from './IntervalTypes';
import LeftPanel from './LeftPanel';
import RightPanel from './RightPanel';

export default class StartTimeSelect extends Component {
  static propTypes = {
    id: PropTypes.string.isRequired,
    startTime: PropTypes.number,
    endTime: PropTypes.number,
    defaultType: PropTypes.string,
    onChange: PropTypes.func.isRequired
  };

  getLabel = () => {
    const selectedInterval = this.getSelectedInterval();
    const selectedType = this.getActiveTimeType(selectedInterval);
    const options = this.getOptions(selectedInterval);
    const startMoment = options.getIn(['range', 'startMoment']);
    const endMoment = options.getIn(['range', 'endMoment']);

    if (selectedType === IntervalTypes.CUSTOM_INTERVAL && startMoment && endMoment) {
      const duration = moment.duration(endMoment.diff(startMoment));
      const days = Math.floor(duration.asDays());
      const rangeText = days > 0
        ? LeftPanel.getDays(endMoment, startMoment)
        : LeftPanel.getHours(endMoment, startMoment);
      return `Custom (${rangeText})`;
    }
    return (!selectedInterval || selectedType === IntervalTypes.ALL_TIME_INTERVAL)
      ? `Start Time : ${selectedInterval.get('label')}`
      : selectedInterval.get('label');
  }

  getOptions(selectedInterval) {
    return Immutable.fromJS({
      range: {
        startMoment: selectedInterval ? selectedInterval.getIn(['time', 0]) : moment(this.props.startTime),
        endMoment: selectedInterval ? selectedInterval.getIn(['time', 1]) : moment(this.props.endTime)
      }
    });
  }

  getSelectedInterval() {
    const startMoment = this.props.startTime && moment(this.props.startTime);
    const endMoment = this.props.endTime && moment(this.props.endTime);
    const intervals = LeftPanel.getIntervals();

    if (!startMoment || !endMoment) {
      return intervals.find(item => item.get('type') === this.props.defaultType);
    }

    return intervals.find(item => {
      const startTime = item.getIn(['time', 0]) && item.getIn(['time', 0]).unix();
      const currentStartTime = startMoment.unix();
      const endTime = item.getIn(['time', 1]).unix();
      const currentEndTime = endMoment.unix();
      const diffStart = Math.abs(currentStartTime - startTime);
      const diffEnd = Math.abs(currentEndTime - endTime);

      return diffStart < 1000 && diffEnd < 1000;
    });
  }
  getActiveTimeType(interval) {
    return interval ? interval.get('type') : IntervalTypes.CUSTOM_INTERVAL;
  }

  handleChange = (type, range) => {
    if (this.props.onChange) {
      this.props.onChange(type, range);
    }
  }

  renderDropdown = () => {
    const selectedInterval = this.getSelectedInterval();
    const selectedType = this.getActiveTimeType(selectedInterval);
    const options = this.getOptions(selectedInterval);

    return (
      <div style={style.dropDown} >
        <LeftPanel
          filterType={this.props.defaultType}
          activeType={selectedType}
          onChange={this.handleChange}
        />
        <RightPanel
          handleChange={this.handleChange}
          options={options}/>
      </div>
    );
  }

  render() {
    return (
      <SelectView
        ref='selectView'
        className={this.props.id}
        content={this.getLabel}
      >
        {this.renderDropdown}
      </SelectView>
    );
  }
}

const style = {
  dropDown: {
    boxShadow: '0 0 5px #999',
    borderRadius: '2px',
    backgroundColor: '#fff',
    overflow: 'hidden',
    zIndex: '99',
    padding: '0 0 8px',
    display: 'flex',
    flexWrap: 'nowrap'
  }
};

