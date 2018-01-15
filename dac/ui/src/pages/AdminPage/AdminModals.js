/*
 * Copyright (C) 2017 Dremio Corporation
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
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import ModalsContainer from 'components/Modals/ModalsContainer';
import * as modals from 'dyn-load/pages/AdminPage/components/modals';

const BODY_CLASS = 'body-modal-open';

@pureRender
export default class AdminModals extends Component {
  static propTypes = {
    children: PropTypes.node
  };

  render() {
    return (
      <ModalsContainer modals={modals} bodyClassName={BODY_CLASS}>
        {this.props.children}
      </ModalsContainer>
    );
  }
}
