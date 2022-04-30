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
import React from 'react';
import Tooltip from '@material-ui/core/Tooltip';
import { FormattedMessage } from 'react-intl';

type TooltipMuiTypes = {
  arrow?: boolean,
  enterDelay?: number,
  enterNextDelay?: number,
  title: string,
  interactive?: boolean,
  children: any
};

const TooltipMui = ({arrow = true, enterDelay = 500, enterNextDelay = 500, title = '', interactive = false, children, ...props}: TooltipMuiTypes) => {

  return (
    !interactive ? // to handle links and other interactions inside tootip
    <Tooltip
      title={<FormattedMessage id={title} defaultMessage={title} />}
      enterDelay={enterDelay}
      enterNextDelay={enterNextDelay}
      arrow={arrow}
      {...props}
    >
      {children}
    </Tooltip> :
    <Tooltip
    title={title}
    enterDelay={enterDelay}
    enterNextDelay={enterNextDelay}
    arrow={arrow}
    interactive={interactive}
    {...props}
  >
    {children}
  </Tooltip>
  );
};

export default TooltipMui;




