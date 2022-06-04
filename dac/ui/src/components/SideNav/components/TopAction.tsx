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

// @ts-ignore
import { Link } from 'react-router';
import Art from '@app/components/Art';
// @ts-ignore
import { Tooltip } from 'dremio-ui-lib';

type TopActionProps = {
  active?: string;
  url: string;
  icon: string;
  alt: string;
  dataqa?: string;
  logo?: boolean;
  socketIsOpen?: boolean;
  tooltipProps?: {};
  tooltip?: boolean;
}


export const TopAction = (props: TopActionProps) => {
  const {
    active = '',
    url,
    icon,
    alt,
    dataqa = 'data-qa',
    logo = false,
    socketIsOpen = true,
    tooltipProps = {},
    tooltip = true
  } = props;

  const shouldHover = logo ? '' : 'item__hover';
  const isSocketOpen = socketIsOpen ? '' : 'socket__notOpen';

  const content = () => {
    return (
      <div className={`${isSocketOpen} sideNav-item ${shouldHover}`}>
        <Link to={url} data-qa={dataqa}>
          <div className={`sideNav-item__link ${active}`}>
            <div className={logo ? 'sideNav-item__logo' : 'sideNav-item__icon'}>
              <Art src={icon} alt={alt} />
            </div>
          </div>
        </Link>
      </div>
    );
  };

  if (tooltip) {
    return (
      <Tooltip title={alt} {...tooltipProps} >
        {content()}
      </Tooltip>
    );
  }

  return content();
};

