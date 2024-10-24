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
import { Component } from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import Immutable from "immutable";

import Modal from "#oss/components/Modals/Modal";
import { getProvision } from "#oss/selectors/provision";
import AdjustWorkersForm from "#oss/pages/AdminPage/subpages/Provisioning/components/forms/AdjustWorkersForm";

export class AdjustWorkersModal extends Component {
  static propTypes = {
    provision: PropTypes.instanceOf(Immutable.Map),
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
  };

  static defaultProps = {
    provision: Immutable.Map(),
  };

  render() {
    const { isOpen, hide, provision } = this.props;

    return (
      <Modal
        title={laDeprecated("Add / Remove Executors")}
        size="smallest"
        isOpen={isOpen}
        hide={hide}
      >
        <AdjustWorkersForm onCancel={hide} entity={provision} />
      </Modal>
    );
  }
}

const mapStateToProps = (state, ownProps) => ({
  provision: getProvision(state, ownProps.entityId),
});

export default connect(mapStateToProps)(AdjustWorkersModal);
