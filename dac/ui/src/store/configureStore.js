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
import { isProduction } from 'utils/config';

import { createStore, applyMiddleware, compose } from 'redux';
import thunk from 'redux-thunk';
import createLogger from 'redux-logger';
import { apiMiddleware } from 'redux-api-middleware';
import { browserHistory } from 'react-router';
import { routerMiddleware } from 'react-router-redux';
import { mockApiMiddleware } from 'mockApi';
import createSagaMiddleware from 'redux-saga';

import rootReducer from 'reducers';

import headerMiddleware from './headerMiddleware';
import lastResponseMiddleware from './lastResponseMiddleware';
import authMiddleware from './authMiddleware';
import serverStatusMiddleware from './serverStatusMiddleware';
import serverErrorMiddleware from './serverErrorMiddleware';
import runSaga from './runSaga';

// todo: actually filter dev-only stuff from prod builds

const sagaMiddleware = createSagaMiddleware();

const isDev = !isProduction();
const middleWares = [
  thunk,
  isDev && mockApiMiddleware,
  headerMiddleware,
  apiMiddleware,
  lastResponseMiddleware,
  authMiddleware,
  serverErrorMiddleware,
  serverStatusMiddleware,
  sagaMiddleware,
  isDev && createLogger({ collapsed: true }),
  routerMiddleware(browserHistory)
].filter(Boolean);

const composeEnhancers = window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__ || compose;

const finalCreateStore = composeEnhancers(...[
  applyMiddleware(...middleWares)
].filter(Boolean))(createStore);

export default function configureStore(initialState) {
  const store = finalCreateStore(rootReducer, initialState);
  runSaga(sagaMiddleware);

  if (isDev && module.hot) {
    // Enable Webpack hot module replacement for reducers
    module.hot.accept('../reducers', () => {
      const nextRootReducer = require('../reducers');
      store.replaceReducer(nextRootReducer);
    });
  }

  return store;
}
