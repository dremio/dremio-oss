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
import Immutable from 'immutable';

import Modal from 'components/Modals/Modal';
import { getEntity } from 'selectors/resources';

import ProvisionInfoTable from '../../subpages/Provisioning/components/ProvisionInfoTable';

export class MoreInfoProvisionModal extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,
    provision: PropTypes.instanceOf(Immutable.Map)
  };

  static defaultProps = {
    provision: Immutable.Map()
  };

  render() {
    const { isOpen, hide, provision } = this.props;

    return (
      <Modal
        title={la('More Info')}
        size='small'
        isOpen={isOpen}
        hide={hide}
      >
        <ProvisionInfoTable provision={provision} />
      </Modal>
    );
  }
}

const mapStateToProps = (state, ownProps) => ({
  provision: getEntity(state, ownProps.entityId, 'provision')
});

export default connect(mapStateToProps)(MoreInfoProvisionModal);
