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
import React, { useState } from 'react';
import PropTypes from 'prop-types';

import Tooltip from '@material-ui/core/Tooltip';

import { ReactComponent as InfoCircleSvg } from '../../art/InfoCircle.svg';
import { ReactComponent as InfoCircleSolid } from '../../art/InfoCircleSolid.svg';

import './HoverHelp.scss';

const HoverHelp = (props) => {
  const [hover, setHover] = useState(false);

  const {
    arrow,
    content,
    placement
  } = props;

  const onMouseEnter = () => {
    setHover(true);
  };
  const onMouseLeave = () => {
    setHover(false);
  };

  const Icon = hover ? InfoCircleSolid : InfoCircleSvg;
  return (
    <div className='hoverHelp margin-left--half'>
      <Tooltip
        arrow={arrow}
        placement={placement}
        title={content}
      >
        <div onMouseEnter={onMouseEnter} onMouseLeave={onMouseLeave}>
          <Icon alt='info'/>
        </div>
      </Tooltip>
    </div>
  );
};

HoverHelp.defaultProps = {
  placement: 'bottom'
};

HoverHelp.propTypes = {
  content: PropTypes.string.isRequired,
  placement: PropTypes.string,
  arrow: PropTypes.bool
};

export default HoverHelp;
