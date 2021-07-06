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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';

import Art from 'components/Art';

import './settingHeader.less';

class SettingHeader extends PureComponent {

  static propTypes = {
    title: PropTypes.string,
    children: PropTypes.node,
    endChildren: PropTypes.node,
    icon: PropTypes.string
  };


  render() {
    const {
      title,
      endChildren,
      icon,
      children
    } = this.props;

    return (
      <div className='settingHeader__root'>
        <div className='settingHeader__title'>
          {icon && <Art src={icon} alt='>' className='settingHeader__icon margin-right'/>}
          {title && <FormattedMessage id={title} defaultMessage={title} />}
          {children}
        </div>
        <div className='settingHeader__endChildren'>
          {endChildren}
        </div>
      </div>
    );
  }
}

export default SettingHeader;
