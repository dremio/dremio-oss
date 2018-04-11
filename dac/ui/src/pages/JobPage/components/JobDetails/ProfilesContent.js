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
import Immutable from 'immutable';

@PureRender
export default class ProfilesContent extends Component {
  static propTypes = {
    jobDetails: PropTypes.instanceOf(Immutable.Map),
    showJobProfile: PropTypes.func
  };

  renderProfiles() {
    const { jobDetails } = this.props;
    if (jobDetails && jobDetails.size) {
      const length = jobDetails.get('attemptDetails').size;
      return jobDetails.get('attemptDetails').reverse().map((profile, i) => {
        const reason = profile.get('reason') ? `(${profile.get('reason')})` : '';
        return (
          <div style={{display: 'flex', marginBottom: 5}} key={i}>
            <div style={{width: 200}}>{la('Attempt')} {length - i} {reason}</div>
            <a onClick={() => this.props.showJobProfile(profile.get('profileUrl'))}>
              {la('Profile')} »</a>
          </div>
        );
      });
    }
  }

  render() {
    return (
      <div style={styles.base} className='profiles'>
        <h4>{la('Attempts')}</h4>
        {this.renderProfiles()}
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    flexDirection: 'column',
    flexWrap: 'nowrap',
    padding: 10
  }
};
