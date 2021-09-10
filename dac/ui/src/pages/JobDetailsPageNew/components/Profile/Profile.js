/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { injectIntl } from 'react-intl';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import './Profile.less';

const Profile = ({
  intl: {
    formatMessage
  },
  jobDetails,
  showJobProfile
}) => {
  const attemptDetails = jobDetails.get('attemptDetails');
  const length = attemptDetails && attemptDetails.size;
  return (
    <div className='profile'>
      <div className='profile__title'>{formatMessage({ id: 'Profile.Attempts' })}</div>
      {
        (attemptDetails && length > 1) && attemptDetails.reverse().map((profile, i) => {
          const reason = profile.get('reason') ? `(${profile.get('reason')})` : '';
          return (
            <div className='profile__rowWrapper' key={i}>
              <span className='profile__rowWrapper__attempt'>
                {formatMessage({ id: 'Profile.Attempt' })} {length - i} {reason}
              </span>
              <a
                className='profile__rowWrapper__profileLink'
                onClick={() => showJobProfile(profile.get('profileUrl'))}
              >
                {formatMessage({ id: 'Job.Profile' })}»
              </a>
            </div>
          );
        })
      }
    </div>
  );
};
Profile.propTypes = {
  intl: PropTypes.object.isRequired,
  jobDetails: PropTypes.instanceOf(Immutable.Map),
  showJobProfile: PropTypes.func
};
export default injectIntl(Profile);
