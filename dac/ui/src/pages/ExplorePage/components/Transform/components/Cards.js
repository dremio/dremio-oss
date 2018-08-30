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
import { Component } from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import { cards } from './Cards.less';

export default class Cards extends Component {

  static propTypes = {
    className: PropTypes.string,
    children: PropTypes.any
  }

  render() {
    const { children, className } = this.props;
    return (
      <div className={classNames('transform-selection', cards, className)}>
        {children}
      </div>
    );
  }
}
