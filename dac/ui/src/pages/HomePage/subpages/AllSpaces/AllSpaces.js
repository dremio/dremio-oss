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
import DocumentTitle from 'react-document-title';
import { injectIntl } from 'react-intl';

import HomePage from 'pages/HomePage/HomePage';

import AllSpacesView from './AllSpacesView.js';

@injectIntl
export class AllSpaces extends PureComponent {
  static propTypes = {
    location: PropTypes.object.isRequired,
    intl: PropTypes.object.isRequired
  };

  render() {
    const { location, intl } = this.props;
    return (
      <HomePage location={location}>
        <DocumentTitle title={intl.formatMessage({ id: 'Space.AllSpaces' })} />
        <AllSpacesView />
      </HomePage>
    );
  }
}
