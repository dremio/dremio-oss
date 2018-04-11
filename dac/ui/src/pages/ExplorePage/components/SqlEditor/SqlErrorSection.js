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
import Immutable from 'immutable';

import FontIcon from 'components/Icon/FontIcon';

import './SqlErrorSection.less';

class SqlErrorSection extends Component {
  static propTypes = {
    visible: PropTypes.bool.isRequired,
    errorData: PropTypes.instanceOf(Immutable.Map).isRequired
  };

  constructor(props) {
    super(props);
    this.state = {
      detailsVisible: false
    };
  }

  getDetailsList(errorData) {
    return (
      errorData.get('details').map((item, index) => {
        return (
          <li key={index}>{item}</li>
        );
      })
    );
  }

  toggleDetails() {
    this.setState({
      detailsVisible: !this.state.detailsVisible
    });
  }

  render() {
    const { visible, errorData } = this.props;
    const holderClasses = classNames('sql-error', {visible});
    const detailsClasses = classNames('details-list', {'visible': this.state.detailsVisible});
    return (
      visible
        ? <div className={holderClasses}>
          <FontIcon type='fa-exclamation-circle'/>
          {errorData.get('msg')}
          <span
            onClick={this.toggleDetails.bind(this)}
            className='details-link'>{this.state.detailsVisible ? 'Hide details' : 'Show details'}</span>
          <ul className={detailsClasses}>{this.getDetailsList(errorData)}</ul>
        </div>
        : null
    );
  }
}

export default SqlErrorSection;
