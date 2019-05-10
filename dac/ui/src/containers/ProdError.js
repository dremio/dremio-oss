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

import { connect } from 'react-redux';

import { hideAppError } from 'actions/prodError';
import ProdErrorModal from 'components/Modals/ProdErrorModal';
import config from 'utils/config';
import { getAppError, getAppErrorId } from '@app/reducers';

export const SHOW_GO_HOME_AFTER_PERIOD = 5000;

export class ProdErrorContainer extends Component {
  static propTypes = {
    error: PropTypes.object,
    errorId: PropTypes.string,
    hideAppError: PropTypes.func
  }

  initTime = Date.now();

  handleHide = () => {
    this.props.hideAppError();
  }

  render() {
    const { error, errorId } = this.props;
    if (!error) {
      return null;
    }
    const showGoHome = Date.now() - this.initTime < SHOW_GO_HOME_AFTER_PERIOD;
    return (
      <ProdErrorModal
        error={error}
        eventId={errorId}
        onHide={this.handleHide}
        showGoHome={showGoHome}
        showFileABug={config.shouldEnableBugFiling}
      />
    );
  }
}

function mapStateToProps(state) {
  return {
    error: getAppError(state),
    errorId: getAppErrorId(state)
  };
}

export default connect(mapStateToProps, {
  hideAppError
})(ProdErrorContainer);
