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
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import { injectIntl } from 'react-intl';

import Art from 'components/Art';

@injectIntl
@Radium
@PureRender
class VisibilityToggler extends Component {
  static propTypes = {
    title: PropTypes.object.isRequired,
    children: PropTypes.node.isRequired,
    isOpen: PropTypes.bool,
    style: PropTypes.object,
    intl: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
    this.toggleVisible = this.toggleVisible.bind(this);
    this.state = {
      isOpen: props.isOpen || false
    };
  }

  toggleVisible() {
    this.setState({
      isOpen: !this.state.isOpen
    });
  }

  render() {
    const { title, style, intl } = this.props;
    const { isOpen } = this.state;
    const iconType = isOpen ? 'TriangleDown.svg' : 'TriangleRight.svg';
    const iconAlt = intl.formatMessage({ id: `Common.${isOpen ? 'Undisclosed' : 'Disclosed'}` });

    return (
      <div style={style}>
        <span onClick={this.toggleVisible} style={styles.title}>
          <Art src={iconType} alt={iconAlt} style={styles.triangle} />
          {title}
        </span>
        {this.state.isOpen ? this.props.children : null}
      </div>
    );
  }
}

export default VisibilityToggler;

const styles = {
  title: {
    display: 'flex',
    alignItems: 'center',
    cursor: 'pointer',
    padding: '2px 0'
  },
  triangle: {
    width: 20,
    height: 20
  }
};
