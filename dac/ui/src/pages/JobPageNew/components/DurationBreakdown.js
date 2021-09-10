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
import DurationProgress from './DurationProgress';

const DurationBreakdown = (
  {
    pending,
    metadataRetrival,
    planning,
    engineStart,
    queued,
    executionPlanning,
    starting,
    running,
    isTotalExecution,
    intl,
    durationTitleClass
  }
) => {
  const totalTime = pending + metadataRetrival + planning + engineStart + queued + executionPlanning + starting + running;
  const durationPercent = isTotalExecution ? 100 : 91;
  const getPercentage = (duration, percent) => {
    return ((duration / totalTime) * percent).toFixed(2);
  };

  return (
    <>
      {
        isTotalExecution && <DurationProgress progressPercentage='100' />
      }
      {
        pending > 0 && <DurationProgress
          title={intl.formatMessage({ id: 'DurationBreakdown.Pending' })}
          time={pending}
          timePercentage={getPercentage(pending, 100)}
          progressPercentage={getPercentage(pending, durationPercent)}
          durationTitleClass={durationTitleClass}
        />
      }
      {
        metadataRetrival > 0 && <DurationProgress
          title={intl.formatMessage({ id: 'DurationBreakdown.Metadata_Retrival' })}
          time={metadataRetrival}
          durationTitleClass={durationTitleClass}
          timePercentage={getPercentage(metadataRetrival, 100)}
          progressPercentage={getPercentage(metadataRetrival, durationPercent)}
          startsFrom={getPercentage(pending, durationPercent)}
        />
      }
      {
        planning > 0 && <DurationProgress
          title={intl.formatMessage({ id: 'DurationBreakdown.Planning' })}
          time={planning}
          durationTitleClass={durationTitleClass}
          timePercentage={getPercentage(planning, 100)}
          progressPercentage={getPercentage(planning, durationPercent)}
          startsFrom={getPercentage(pending + metadataRetrival, durationPercent)}
        />
      }
      {
        engineStart > 0 && <DurationProgress
          title={intl.formatMessage({ id: 'DurationBreakdown.Engine_Start' })}
          time={engineStart}
          durationTitleClass={durationTitleClass}
          timePercentage={getPercentage(engineStart, 100)}
          progressPercentage={getPercentage(engineStart, durationPercent)}
          startsFrom={getPercentage(pending + metadataRetrival + planning, durationPercent)}
        />
      }
      {
        queued > 0 && <DurationProgress
          title={intl.formatMessage({ id: 'DurationBreakdown.Queued' })}
          time={queued}
          durationTitleClass={durationTitleClass}
          timePercentage={getPercentage(queued, 100)}
          progressPercentage={getPercentage(queued, durationPercent)}
          startsFrom={getPercentage(pending + metadataRetrival + planning + engineStart, durationPercent)}
        />
      }
      {
        executionPlanning > 0 && <DurationProgress
          title={intl.formatMessage({ id: 'DurationBreakdown.Execution_Planning' })}
          time={executionPlanning}
          durationTitleClass={durationTitleClass}
          timePercentage={getPercentage(executionPlanning, 100)}
          progressPercentage={getPercentage(executionPlanning, durationPercent)}
          startsFrom={getPercentage(
            pending +
            metadataRetrival +
            planning +
            engineStart +
            queued,
            durationPercent
          )}
        />
      }
      {
        starting > 0 && <DurationProgress
          title={intl.formatMessage({ id: 'DurationBreakdown.Starting' })}
          time={starting}
          durationTitleClass={durationTitleClass}
          timePercentage={getPercentage(starting, 100)}
          progressPercentage={getPercentage(starting, durationPercent)}
          startsFrom={getPercentage(
            pending +
            metadataRetrival +
            planning +
            engineStart +
            queued +
            executionPlanning,
            durationPercent
          )}
        />
      }
      {
        running > 0 && <DurationProgress
          title={intl.formatMessage({ id: 'DurationBreakdown.Running' })}
          time={running}
          durationTitleClass={durationTitleClass}
          timePercentage={getPercentage(running, 100)}
          progressPercentage={getPercentage(running, durationPercent)}
          startsFrom={getPercentage(
            pending +
            metadataRetrival +
            planning +
            engineStart +
            queued +
            executionPlanning +
            starting,
            durationPercent
          )}
        />
      }
    </>
  );
};

DurationBreakdown.propTypes = {
  pending: PropTypes.number,
  metadataRetrival: PropTypes.number,
  planning: PropTypes.number,
  engineStart: PropTypes.number,
  queued: PropTypes.number,
  executionPlanning: PropTypes.number,
  starting: PropTypes.number,
  running: PropTypes.number,
  isTotalExecution: PropTypes.bool,
  intl: PropTypes.object.isRequired,
  durationTitleClass: PropTypes.string
};

DurationBreakdown.defaultProps = {
  pending: 0,
  metadataRetrival: 0,
  planning: 0,
  engineStart: 0,
  queued: 0,
  executionPlanning: 0,
  starting: 0,
  running: 0,
  isTotalExecution: false
};

export default injectIntl(DurationBreakdown);

