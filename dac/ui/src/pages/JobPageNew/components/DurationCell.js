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
import { useState, useRef, useEffect } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { injectIntl } from 'react-intl';

import { Tooltip } from '@app/components/Tooltip';
import DurationBreakdown from '@app/pages/JobPageNew/components/DurationBreakdown';
import { getDuration } from 'utils/jobListUtils';
import Art from 'components/Art';

import './JobsContent.less';

const DurationCell = ({
  duration,
  durationDetails,
  isSpilled,
  intl
}) => {
  const [tooltipOpen, setTooltipOpen] = useState(false);
  const durationRef = useRef(null);

  useEffect(() => {
    const timer = setTimeout(() => setTooltipOpen(false), 3000);
    return () => clearTimeout(timer);
  }, [tooltipOpen]);

  const handleMouseEnter = () => {
    setTooltipOpen(true);
  };

  const handleMouseLeave = () => {
    setTooltipOpen(false);
  };

  return (
    <div data-qa='durationCell' ref={durationRef} onMouseEnter={handleMouseEnter} onMouseLeave={handleMouseLeave}>
      <div className='jobsContent__twoColumnWrapper jobsContent__numericContent'>
        <div className='jobsContent__durationSpill'>
          <span>{duration}</span>
          <span>{isSpilled &&
            <Art src='DiskSpill.svg' alt={intl.formatMessage({ id: 'Job.SpilledHover' })}
              className='jobsContent__spillIcon' />}
          </span>
        </div>
      </div>
      <Tooltip key='tooltip'
        target={() => tooltipOpen ? durationRef.current : null}
        placement='bottom-start'
        type='custom'
        className='jobsContent__tooltip'
        tooltipInnerStyle={styles.tooltipInnerStyle}
        tooltipArrowClass='textWithHelp__tooltipArrow --light'
      >
        {/* TODO: props values need to be populated from API response */}
        <DurationBreakdown
          pending={getDuration(durationDetails, 'PENDING')}
          metadataRetrival={getDuration(durationDetails, 'METADATA_RETRIEVAL')}
          planning={getDuration(durationDetails, 'PLANNING')}
          engineStart={getDuration(durationDetails, 'ENGINE_START')}
          queued={getDuration(durationDetails, 'QUEUED')}
          executionPlanning={getDuration(durationDetails, 'EXECUTION_PLANNING')}
          starting={getDuration(durationDetails, 'STARTING')}
          running={getDuration(durationDetails, 'RUNNING')}
        />
      </Tooltip>
    </div>
  );
};

DurationCell.propTypes = {
  duration: PropTypes.string,
  durationDetails: PropTypes.instanceOf(Immutable.List),
  isSpilled: PropTypes.bool,
  intl: PropTypes.object.isRequired
};

DurationCell.defaultProps = {
  duration: '',
  durationDetails: Immutable.List(),
  isSpilled: false
};

const styles = {
  tooltipInnerStyle: {
    width: '425px',
    maxWidth: '533px',
    whiteSpace: 'nowrap',
    background: '#F4FAFC', //DX-34369
    border: '1.5px solid #43B8C9',
    padding: '2px 18px 18px 18px'
  },
  iconStyle: {
    height: '25px'
  }
};

export default injectIntl(DurationCell);
