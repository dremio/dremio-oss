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
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';
import Immutable from 'immutable';

import SimpleButton from 'components/Buttons/SimpleButton';
import EllipsedText from 'components/EllipsedText';
import { ExploreInfoHeader } from 'pages/ExplorePage/components/ExploreInfoHeader';


const CLEAR_ALL_HEIGHT = 20;

@pureRender
@Radium
class AggregateHeader extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    style: PropTypes.object,
    onClearAllMeasures: PropTypes.func,
    onClearAllDimensions: PropTypes.func
  };

  /**
   * Renders `Clear All` button when needed, otherwise nothing
   *
   * @param  {Function} clearFunction clear items function to be called on button click
   * @return {React.Element} element to render
   */
  renderClearAll(clearFunction) {
    if (clearFunction) {
      return <SimpleButton
        type='button'
        buttonStyle='secondary'
        style={styles.clearAll}
        onClick={clearFunction}>
        {la('Clear All')}
      </SimpleButton>;
    }
    return null;
  }

  render() {
    // todo: loc
    const nameForDisplay = ExploreInfoHeader.getNameForDisplay(this.props.dataset);
    return (
      <div
        className='aggregate-header'
        style={[styles.base, this.props.style]}>
        <div style={styles.left}><EllipsedText text={`“${nameForDisplay}” dataset fields:`}/></div>
        <div style={styles.center}>
          {la('Dimensions')}
          {this.renderClearAll(this.props.onClearAllDimensions)}
        </div>
        <div style={styles.right}>
          {la('Measures')}
          {this.renderClearAll(this.props.onClearAllMeasures)}
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    justifyContent: 'space-between',
    width: '100%',
    alignItems: 'center',
    background: '#F3F3F3',
    minHeight: 30
  },
  left: {
    width: 275,
    minWidth: 240,
    height: 30,
    display: 'flex',
    alignItems: 'center',
    padding: '0 10px',
    borderRight: '1px solid rgba(0,0,0,0.10)'
  },
  center: {
    width: '100%',
    height: 30,
    display: 'flex',
    alignItems: 'center',
    paddingLeft: 11,
    borderRight: '1px solid rgba(0,0,0,0.10)',
    justifyContent: 'space-between'
  },
  right: {
    height: 30,
    display: 'flex',
    alignItems: 'center',
    paddingLeft: 10,
    width: '100%',
    justifyContent: 'space-between'
  },
  clearAll: {
    minWidth: 'auto',
    height: CLEAR_ALL_HEIGHT,
    lineHeight: `${CLEAR_ALL_HEIGHT}px`,
    marginRight: 10
  }
};

export default AggregateHeader;
