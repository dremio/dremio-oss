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
import { Link, withRouter } from 'react-router';
import { compose } from 'redux';
import { injectIntl } from 'react-intl';
import datasetPathUtils from '@app/utils/resourcePathUtils/dataset';
import jobsUtils, { JobState } from '@app/utils/jobsUtils';
import { getTabs, getIconName } from 'dyn-load/utils/jobsUtils';
import * as ButtonTypes from '@app/components/Buttons/ButtonTypes';
import Button from '@app/components/Buttons/Button';
import { constructFullPathAndEncode, constructResourcePath } from '@app/utils/pathUtils';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import JobStateIcon from '@app/pages/JobPage/components/JobStateIcon';
import Art from '@app/components/Art';
import TopPanelTab from './TopPanelTab.js';
import './TopPanel.less';

const renderIcon = (iconName, className, selected) => {
  return (<Art
    src={iconName}
    alt='icon'
    className={classNames(
      'topPanel__icons',
      className,
      { '--lightBlue': selected })
    }
  />);
};

export const TopPanel = (props) => {
  const {
    intl: {
      formatMessage
    },
    jobId,
    breadcrumbRouting,
    router,
    location,
    setComponent,
    jobStatus,
    showJobProfile,
    cancelJob,
    jobDetails
  } = props;

  const renderOpenResults = () => {

    if (jobsUtils.getRunning(jobStatus) || jobStatus === JobState.ENQUEUED
      || jobStatus === JobState.PLANNING) {
      return (
        <Button
          type={ButtonTypes.CUSTOM}
          text={formatMessage({ id: 'Common.Cancel' })}
          onClick={() => cancelJob(jobId)}
        />);
    }

    const queryType = jobDetails.get('queryType');
    if (!jobDetails.get('resultsAvailable') || jobStatus !== JobState.COMPLETED
      || (queryType !== 'UI_PREVIEW' && queryType !== 'UI_RUN')) {
      return null;
    }

    const QueriedDataset = jobDetails.get('queriedDatasets');
    const datasetFullPath = QueriedDataset.get(0).get('datasetPathsList');
    let fullPath;

    if (datasetFullPath && datasetFullPath.size > 0) {
      fullPath = `${datasetFullPath.get(0)}.${constructFullPathAndEncode(datasetFullPath.slice(1))}`;
    } else {
      fullPath = constructFullPathAndEncode(datasetFullPath);
    }
    const resourcePath = constructResourcePath(fullPath);
    const nextLocation = {
      pathname: datasetFullPath && datasetFullPath.size > 0 ? datasetPathUtils.toHref(resourcePath) : 'tmp/UNTITLED',
      query: { jobId, version: jobDetails.get('datasetVersion'), openResults: 'true' }
    };

    if (QueriedDataset.get('datasetPathsList')) {
      nextLocation.query.mode = 'edit';
    }

    return (
      <Link data-qa='open-results-link' to={nextLocation}>
        {formatMessage({ id: 'Job.OpenResults' })} »
      </Link>
    );
  };

  const [selectedTab, setSelectedTab] = useState('Overview');

  const onTabClick = (tab, moreAttempts) => {
    if (tab === 'Profile' && !moreAttempts) {
      setSelectedTab(tab);
      isSingleProfile ? showJobProfile(profileUrl) : onTabClick('Profile', true);
    } else {
      setComponent(tab);
      setSelectedTab(tab);
    }
  };

  const changePages = () => {
    const { state } = location;
    state && state.history
      ? breadcrumbRouting()
      : router.push({
        ...location,
        pathname: '/jobs'
      });
  };

  const attemptDetails = jobDetails && jobDetails.get('attemptDetails');
  const profileUrl = attemptDetails && attemptDetails.getIn([0, 'profileUrl']);
  const isSingleProfile = attemptDetails && attemptDetails.size === 1;
  const tabs = getTabs();
  return (
    <div className='topPanel'>
      <div className='topPanel__navigationWrapper'>
        <div className='topPanel__jobDetails'>
          <div data-qa='jobs-logo' onClick={changePages}>
            {renderIcon('Jobs.svg', 'topPanel__jobDetails__jobsIcon')}
          </div>
          <div className='gutter-top--half'>
            <JobStateIcon state={jobStatus} />
          </div>
          <div data-qa='top-panel-jobId' className='topPanel__jobId'>{jobId}</div>
        </div>
        {
          tabs.map((tab, index) => {

            return (
              <TopPanelTab
                tabName={tab}
                onTabClick={onTabClick}
                selectedTab={selectedTab}
                iconName={getIconName(tab)}
                key={`${tab}-${index}`}
              />
            );
          })
        }
      </div>
      <span className='topPanel__openResults'>
        {renderOpenResults()}
      </span>
    </div>
  );
};

TopPanel.propTypes = {
  intl: PropTypes.object.isRequired,
  breadcrumbRouting: PropTypes.func,
  jobId: PropTypes.string,
  setComponent: PropTypes.func,
  jobDetails: PropTypes.object,
  showJobProfile: PropTypes.func,
  jobStatus: PropTypes.string,
  cancelJob: PropTypes.func,
  router: PropTypes.object.isRequired,
  location: PropTypes.object.isRequired
};

export default compose(
  withRouter,
  injectIntl)(TopPanel);

