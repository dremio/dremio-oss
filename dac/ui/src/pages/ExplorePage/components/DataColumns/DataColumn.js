/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import classNames from 'classnames';
import { formatMessage } from 'utils/locale';
import { typeToIconType, typeToFormatMessageId } from '@app/constants/DataTypes';
import Art from '@app/components/Art';
import {
  name as nameCls,
  icon as iconCls,
  wrapper
} from './DataColumn.less';

export const columnPropTypes = {
  type: PropTypes.string, //see constants/DataTypes for the list of available types
  name: PropTypes.string
};

export class DataColumn extends PureComponent {
  static propTypes = {
    ...columnPropTypes,
    className: PropTypes.string
  };

  render() {
    const {
      type,
      name,
      className
    } = this.props;

    return (<div className={classNames(wrapper, className)}>
      <Art
        src={`types/${typeToIconType[type]}.svg`}
        alt={formatMessage(`${typeToFormatMessageId[type]}`)}
        className={iconCls}
        />
      <div className={nameCls}>{name}</div>
    </div>);
  }
}
