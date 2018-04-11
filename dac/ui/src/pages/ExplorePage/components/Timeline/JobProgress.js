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
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import Spinner from 'components/Spinner';

import './JobProgress.less';

@pureRender
export default class JobProgress extends Component {
  static propTypes = {
    start: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    end: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    hideSpinner: PropTypes.bool
  };

  constructor(props) {
    super(props);
  }

  getCurrentProgress() {
    if (!this.props.end) {
      return (
        <div>
          <span className='progress-message'>Job Inprogress</span>
          <span className='progress-view-jobs'>View running jobs (3) »</span>
        </div>
      );
    }

    const totalWidth = this.refs.total && this.refs.total.offsetWidth || 180;
    const currentProgress = {
      width: (this.props.start / this.props.end) * totalWidth
    };

    return (
      <div>
        <span className='result'>
          <span className='end-items'>{this.props.start}</span>
          of
          <span className='all-items'>{this.props.end}</span>
          records
        </span>
        <div className='progress-line' ref='total'/>
        <div className='current-progress' style={currentProgress}/>
      </div>
    );
  }

  render() {
    return (
      <div className='job-progress' onClick={this.toogleDropdown}>
        <div className='spinner-part'>
          <Spinner style={{display: this.props.hideSpinner ? 'none' : 'block'}}/>
          Processing...
        </div>
        <div className='progress-part'>
          {this.getCurrentProgress()}
        </div>
      </div>
    );
  }
}
