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
import jobsUtils from '@app/utils/jobsUtils';
import './DurationProgress.less';

const DurationProgress = (
  {
    title,
    time,
    timePercentage,
    progressPercentage,
    startsFrom,
    durationTitleClass
  }
) => {
  return (
    <div className='durationProgress'>
      {
        title && time && <div className='durationProgress-content'>
          <span className={durationTitleClass || 'durationProgress-content__title'}>
            {title}
          </span>
          <span
            data-qa='duration-breakdown-value'
            className='durationProgress-content__value'>
            {time < 1 ? '<1s' : jobsUtils.formatJobDuration(time * 1000)}({timePercentage}%)
          </span>
        </div>
      }
      <div className='durationProgress-progress'>
        <div
          className='durationProgress-progress__value'
          style={{ width: `${progressPercentage}%`, marginLeft: startsFrom ? `${startsFrom}%` : 0 }} />
      </div>
    </div>
  );
};

DurationProgress.propTypes = {
  title: PropTypes.string,
  time: PropTypes.number,
  timePercentage: PropTypes.string,
  progressPercentage: PropTypes.string,
  startsFrom: PropTypes.string,
  durationTitleClass: PropTypes.string
};

DurationProgress.defaultProps = {
  title: '',
  time: 0,
  timePercentage: '',
  progressPercentage: '',
  startsFrom: ''
};

export default DurationProgress;

