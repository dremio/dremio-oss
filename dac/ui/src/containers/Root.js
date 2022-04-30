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
import PropTypes from 'prop-types';

import { RawIntlProvider } from 'react-intl';
import { Provider } from 'react-redux';

import { Router, browserHistory } from 'react-router';
import { syncHistoryWithStore } from 'react-router-redux';
import { useProjectContext } from '@inject/utils/storageUtils/localStorageUtils';
import routes from 'routes';

import { intl } from '@app/utils/intl';
import intercomUtils from 'utils/intercomUtils';
import { oc } from 'ts-optchain';

function Root({ store }) {
  const history = syncHistoryWithStore(browserHistory, store);
  history.listen(() => {
    intercomUtils.update();
  });
  const projectContext = useProjectContext();
  //Re-render routes when project context changes
  const renderKey = oc(projectContext).id('root');

  return <RawIntlProvider value={intl}>
    <Provider store={store}>
      <div style={{height: '100%'}}>
        <Router key={renderKey} history={history}>{routes(store.dispatch, projectContext)}</Router>
      </div>
    </Provider>
  </RawIntlProvider>;
}
Root.propTypes = {
  store: PropTypes.object.isRequired
};
export default Root;
