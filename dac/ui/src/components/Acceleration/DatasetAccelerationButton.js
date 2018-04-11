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
import FontIcon from 'components/Icon/FontIcon';
import { getDatasetAcceleration } from 'selectors/resources';
import { constructFullPathAndEncode } from 'utils/pathUtils';

import {
  getDatasetAccelerationRequest
} from 'dyn-load/actions/resources/accelerationRequest';

export class DatasetAccelerationButton extends Component {
  static propTypes = {
    fullPath: PropTypes.instanceOf(Immutable.List),
    acceleration: PropTypes.instanceOf(Immutable.Map),
    getDatasetAccelerationRequest: PropTypes.func
  }

  static defaultProps = {
    acceleration: Immutable.Map()
  }

  componentWillMount() {
    this.props.getDatasetAccelerationRequest(constructFullPathAndEncode(this.props.fullPath));
  }

  render() {
    if (!this.props.acceleration.get('enabled')) {
      return null;
    }
    return (
      <div style={styles.base}>
        <div style={styles.content}>
          <FontIcon theme={styles.icon} type={'Flame'} />
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state, ownProps) => {
  const acceleration = getDatasetAcceleration(state, ownProps.fullPath);
  return {
    acceleration
  };
};

export default connect(mapStateToProps, {
  getDatasetAccelerationRequest
})(DatasetAccelerationButton);

const styles = {
  base: {
    position: 'relative'
  },
  content: {
    minWidth: 30,
    textAlign: 'center'
  },
  icon: {
    'Icon': {
      width: 13,
      height: 20
    }
  }
};
