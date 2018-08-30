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
import Immutable from 'immutable';
import FontIcon from 'components/Icon/FontIcon';
import CardFooter from './CardFooter';

export default class TransformCard extends Component {

  static propTypes = {
    front: PropTypes.node,
    back: PropTypes.node,
    card: PropTypes.instanceOf(Immutable.Map),
    active: PropTypes.bool,
    onClick: PropTypes.func
  };

  constructor(props) {
    super(props);

    const hasExamples = props.card && props.card.get('examplesList') && props.card.get('examplesList').size;

    this.state = {
      editing: !hasExamples
    };
  }

  componentWillReceiveProps(nextProps) {
    if (!nextProps.active) {
      this.setState({ editing: false });
    }
  }

  onToggleEdit = () => {
    this.setState({ editing: !this.state.editing });
  }

  render() {
    const { front, back, card, active, onClick } = this.props;
    const { editing } = this.state;
    const footerStyle = { width: styles.base.width, backgroundColor: styles.base.backgroundColor };
    const backgroundColor = active ? '#FFF5DC' : '#FFFFFF';
    return (
      <div
        className='transform-card'
        onClick={onClick}
        style={{ ...styles.base, ...(active ? styles.active : {}) }}>
        {(editing || !front) ? back : front}
        {front && <FontIcon
          type={editing ? 'Return' : 'EditSmall'}
          theme={{ Icon: styles.toggler }}
          onClick={this.onToggleEdit}/>}
        <CardFooter card={card} style={{ ...footerStyle, backgroundColor }}/>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'space-between',
    height: 150,
    width: 455,
    minWidth: 455,
    position: 'relative',
    backgroundColor: '#FFFFFF',
    cursor: 'pointer',
    ':hover': {
      backgroundColor: '#FFF5DC'
    }
  },
  active: {
    backgroundColor: '#FFF5DC'
  },
  toggler: {
    position: 'absolute',
    top: 4,
    right: 10,
    cursor: 'pointer',
    fontSize: 17,
    ':hover': {
      color: 'gray'
    }
  }
};
