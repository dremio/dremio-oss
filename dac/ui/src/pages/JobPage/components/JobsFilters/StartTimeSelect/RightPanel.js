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
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import * as IntervalTypes from './IntervalTypes';
import RightPanelView from './RightPanelView';

@Radium
@PureRender
export default class RightPanel extends Component {
  static propTypes = {
    handleChange: PropTypes.func.isRequired,
    options: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
    this.handleChange = this.handleChange.bind(this);
    this.onChangeInInput = this.onChangeInInput.bind(this);
  }

  onChangeInInput(type, date) {
    const nextRange = type === 'startDate'
      ? [ date, this.props.options.getIn(['range', 'endMoment']) ]
      : [ this.props.options.getIn(['range', 'startMoment']), date ];
    this.props.handleChange(IntervalTypes.CUSTOM_INTERVAL, Immutable.fromJS(nextRange));
  }

  handleChange(date, isInit) {
    const endMoment = this.props.options.getIn(['range', 'endMoment']);
    const startMoment = this.props.options.getIn(['range', 'startMoment']);
    const isDateChanged = endMoment.unix() !== date.endDate.unix() || startMoment.unix() !== date.startDate.unix();
    const isRange = date.endDate.unix() !== date.startDate.unix();
    if (!isInit && isDateChanged && isRange) {
      this.props.handleChange(IntervalTypes.CUSTOM_INTERVAL, Immutable.fromJS([
        date.startDate,
        date.endDate
      ]));
    }
  }

  render() {
    const { options } = this.props;
    const range = options.get('range');
    const startMoment = range.get('startMoment');
    const endMoment = range.get('endMoment');
    return (
      <RightPanelView
        startMoment={startMoment}
        endMoment={endMoment}
        onChange={this.handleChange}
        onChangeInInput={this.onChangeInInput}
      />
    );
  }
}
