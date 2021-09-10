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
import { useState } from 'react';
import { injectIntl } from 'react-intl';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import JobStateIcon from '@app/pages/JobPage/components/JobStateIcon';
import Art from '@app/components/Art';
import './TopPanel.less';

const renderIcon = (iconName, className) => {
  return (<Art
    src={iconName}
    alt='icon'
    title='icon'
    className={classNames('topPanel__icons', className)}
  />);
};

export const TopPanel = (props) => {
  const {
    intl: {
      formatMessage
    },
    jobId,
    changePages,
    setComponent,
    jobStatus,
    showJobProfile,
    jobDetails
  } = props;

  const [selectedTab, setSelectedTab] = useState('Overview');

  const onTabClick = (tab) => {
    setComponent(tab);
    setSelectedTab(tab);
  };

  const attemptDetails = jobDetails && jobDetails.get('attemptDetails');
  const profileUrl = attemptDetails && attemptDetails.getIn([0, 'profileUrl']);
  const isSingleProfile = attemptDetails && attemptDetails.size === 1;
  return (
    <div className='topPanel'>
      <div className='topPanel__jobDetails'>
        <div data-qa='jobs-logo' onClick={() => changePages(null)}>
          {renderIcon('Jobs.svg', 'topPanel__jobDetails__jobsIcon')}
        </div>
        <div className='gutter-top--half'>
          <JobStateIcon state={jobStatus} />
        </div>
        <div data-qa='top-panel-jobId'>{jobId}</div>
      </div>
      <div
        className='topPanel__overview'
        onClick={() => onTabClick('Overview')}
      >
        <div className={classNames(
          'topPanel__overview__content',
          { 'topPanel__border': selectedTab === 'Overview' }
        )}>
          {selectedTab === 'Overview' ?
            renderIcon('Shape_lite.svg', 'topPanel__overview__overviewIcon')
            :
            renderIcon('Shape_lite.svg')
          }
          <div className={
            selectedTab === 'Overview'
              ?
              'topPanel__overview__headerLite'
              :
              'topPanel__overview__header'
          }>
            {formatMessage({ id: 'TopPanel.Overview' })}
          </div>
        </div>
      </div>

      <div className='topPanel__sql'
        data-qa='toppanel-sql'
        onClick={() => onTabClick('SQL')}>
        <div className={classNames(
          'topPanel__sql__content',
          { 'topPanel__border': selectedTab === 'SQL' }
        )}>
          {selectedTab === 'SQL' ?
            renderIcon('Union.svg', 'topPanel__sql__sqlIcon')
            :
            renderIcon('Union.svg')
          }
          <div className={
            selectedTab === 'SQL'
              ?
              'topPanel__overview__headerLite'
              :
              'topPanel__overview__header'
          }>{formatMessage({ id: 'TopPanel.SQL' })}</div>
        </div>
      </div>

      <div
        className='topPanel__rawProfile'
        onClick={() => isSingleProfile ? showJobProfile(profileUrl) : onTabClick('Profile')}
      >
        <div className={classNames(
          'topPanel__rawProfile__content',
          { 'topPanel__border': selectedTab === 'Profile' }
        )}>
          {selectedTab === 'Profile' ?
            renderIcon('RawProfile.svg', 'topPanel__rawProfile__rawProfileSelectedIcon')
            :
            renderIcon('RawProfile.svg', 'topPanel__rawProfile__rawProfileIcon')
          }
          <div className={
            selectedTab === 'Profile'
              ?
              'topPanel__rawProfile__headerLite'
              :
              'topPanel__rawProfile__header'
          }>
            {formatMessage({ id: 'TopPanel.Rawprofile' })}
          </div>
        </div>
      </div>
    </div>
  );
};

TopPanel.propTypes = {
  intl: PropTypes.object.isRequired,
  changePages: PropTypes.func,
  jobId: PropTypes.string,
  setComponent: PropTypes.func,
  jobDetails: PropTypes.object,
  showJobProfile: PropTypes.func,
  jobStatus: PropTypes.string
};

export default injectIntl(TopPanel);
