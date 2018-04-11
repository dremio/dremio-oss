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
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

@PureRender
export default class RealTimeTimer extends Component {
  static propTypes = {
    startTime: PropTypes.number.isRequired,
    updateInterval: PropTypes.number,
    formatter: PropTypes.func
  };

  static defaultProps = {
    updateInterval: 1000,
    formatter: (data) => data
  }

  intervalId = null; // eslint-disable-line react/sort-comp

  componentDidMount() {
    this.startTimer();
  }

  componentWillUnmount() {
    clearInterval(this.intervalId);
  }

  startTimer() {
    clearInterval(this.intervalId);
    this.intervalId = setInterval(() => {
      this.forceUpdate();
    }, this.props.updateInterval);
  }

  render() {
    return (
      <span >
        {this.props.formatter(Date.now() - this.props.startTime)}
      </span>
    );
  }
}
