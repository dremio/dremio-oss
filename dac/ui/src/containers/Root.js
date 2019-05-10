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

import { Provider } from 'react-redux';

import { Router, browserHistory } from 'react-router';
import { syncHistoryWithStore } from 'react-router-redux';
import routes from 'routes';

import intercomUtils from 'utils/intercomUtils';

import { addLocaleData, IntlProvider } from 'react-intl';
import enData from 'react-intl/locale-data/en';
import { getLocale } from '../utils/locale';

addLocaleData([...enData]);

export default class Root extends Component {

  static propTypes = {
    store: PropTypes.object.isRequired
  };

  render() {
    const { store } = this.props;
    const history = syncHistoryWithStore(browserHistory, store);
    history.listen(() => {
      intercomUtils.update();
    });

    return <IntlProvider locale={getLocale().language} messages={getLocale().localeStrings}>
      <Provider store={store}>
        <div style={{height: '100%'}}>
          <Router history={history}>{routes(store.dispatch)}</Router>
        </div>
      </Provider>
    </IntlProvider>;
  }
}
