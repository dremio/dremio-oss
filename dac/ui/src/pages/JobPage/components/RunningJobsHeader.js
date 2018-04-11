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
import PropTypes from 'prop-types';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import { PALE_NAVY } from 'uiTheme/radium/colors';

@Radium
@PureRender
export default class RunningJobsHeader extends Component {
  static propTypes = {
    jobCount: PropTypes.number.isRequired
  };

  //TODO: jobCount will be used after beta2
  render() {
    return (
      <h3 className='running-jobs-header' style={[style.base]}>
        Jobs
      </h3>
    );
  }
}

const style = {
  base: {
    backgroundColor: PALE_NAVY,
    flexShrink: 0,
    display: 'flex',
    alignItems: 'center',
    height: 38,
    padding: '0 10px'
  }
};
