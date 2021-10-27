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
import PropTypes from 'prop-types';
import { injectIntl } from 'react-intl';
import DurationBreakdown from '@app/pages/JobPageNew/components/DurationBreakdown';
import jobsUtils from '@app/utils/jobsUtils';
import './TotalExecutionTime.less';

const TotalExecutionTime = (
  {
    pending,
    metadataRetrival,
    planning,
    engineStart,
    queued,
    executionPlanning,
    starting,
    running,
    total,
    intl
  }
) => {
  return (
    <div className='totalExecutionTime'>
      <div className='totalExecutionTime__titleWrapper'>
        <div className='totalExecutionTime__title' >
          {intl.formatMessage({ id: 'TotalExecutionTime' })}
        </div>
        <div className='totalExecutionTime__content'>
          {jobsUtils.formatJobDurationWithMS(total)} (100%)
        </div>
      </div>
      <DurationBreakdown
        isTotalExecution
        pending={pending}
        metadataRetrival={metadataRetrival}
        planning={planning}
        engineStart={engineStart}
        queued={queued}
        executionPlanning={executionPlanning}
        starting={starting}
        running={running}
        durationTitleClass='totalExecutionTime__progressTitle'
      />
    </div>
  );
};

TotalExecutionTime.propTypes = {
  pending: PropTypes.number,
  metadataRetrival: PropTypes.number,
  planning: PropTypes.number,
  engineStart: PropTypes.number,
  queued: PropTypes.number,
  executionPlanning: PropTypes.number,
  starting: PropTypes.number,
  running: PropTypes.number,
  total: PropTypes.number,
  intl: PropTypes.object.isRequired
};

TotalExecutionTime.defaultProps = {
  pending: 0,
  metadataRetrival: 0,
  planning: 0,
  engineStart: 0,
  queued: 0,
  executionPlanning: 0,
  starting: 0,
  running: 0,
  total: 0
};

export default injectIntl(TotalExecutionTime);
