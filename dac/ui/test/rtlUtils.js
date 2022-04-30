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
import { render as rtlRender } from '@testing-library/react';
import { Provider } from 'react-redux';
import configureStore from 'store/configureStore';

export function render(ui, { initialState = {}} = {}) {
  const store = configureStore(initialState);
  // eslint-disable-next-line
  function wrapper(ui) {
    return (
      <Provider store={store}>
        {ui}
      </Provider>
    );
  }
  const utils = rtlRender(wrapper(ui));

  return {
    ...utils,
    rerender: rerenderedUi => utils.rerender(wrapper(rerenderedUi)),
    store,
    history
  };
}
