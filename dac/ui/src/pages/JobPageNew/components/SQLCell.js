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
import { Tooltip } from '@app/components/Tooltip';
import './JobsContent.less';

const SQLCell = ({ sql }) => {
  const [tooltipOpen, setTooltipOpen] = useState(false);
  const sqlRef = useRef(null);

  const TooltipInnerStyle = {
    width: 'auto',
    maxWidth: '33.31rem',
    maxHeight: '16.5rem',
    background: '#F4FAFC', //DX-34369
    border: '1.5px solid #43B8C9',
    padding: '12px 15px 12px 15px'
  };

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
    <div
      data-qa='SQLCell'
      ref={sqlRef}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
    >
      <div className='jobsContent__sqlWrapper'>{sql}</div>
      <Tooltip key='tooltip'
        target={() => tooltipOpen ? sqlRef.current : null}
        placement='bottom-start'
        type='custom'
        className='jobsContent__tooltip'
        tooltipInnerStyle={TooltipInnerStyle}
        tooltipArrowClass='textWithHelp__tooltipArrow --light'
      >
        <div className='jobsContent__sqlTooltipText'>{sql}</div>
      </Tooltip>
    </div>
  );
};

SQLCell.propTypes = {
  sql: PropTypes.string
};

SQLCell.defaultProps = {
  sql: ''
};

export default SQLCell;
