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
import { Component, PropTypes } from 'react';
import { connect } from 'react-redux';
import Immutable from 'immutable';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import FontIcon from 'components/Icon/FontIcon';

import { toggleExploreSql } from 'actions/explore/ui';

import { formLabel } from 'uiTheme/radium/typography';

@pureRender
@Radium
export class SqlToggle extends Component {

  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    style: PropTypes.object,

    //connected
    toggleExploreSql: PropTypes.func.isRequired,
    sqlState: PropTypes.bool
  };

  static contextTypes = {
    location: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired
  };

  handleToggleClick = () => {
    this.props.toggleExploreSql();
  };

  render() {
    const {dataset, sqlState, style} = this.props;
    const canToggle = !dataset.get('isNewQuery');
    const directionIconType = sqlState ? 'TriangleDown' : 'TriangleRight';
    return (
      <div
        className='sql-toggle'
        data-qa='sql-toogle'
        onClick={canToggle ? this.handleToggleClick : undefined}
        style={[styles.base, canToggle && {cursor: 'pointer', marginLeft: sqlState ? 5 : 0}, style]}>
        { canToggle &&
          <FontIcon
            type={directionIconType}
            theme={styles.iconStyle}/>
        }
        <span style={[formLabel]}>
          {la('SQL Editor')}
        </span>
      </div>
    );
  }
}

export default connect(null, {
  toggleExploreSql
})(SqlToggle);


const styles = {
  base: {
    display: 'flex',
    alignItems: 'center',
    marginRight: 5,
    marginLeft: 10
  },
  iconStyle: {
    Icon: {
      width: 24,
      height: 24
    },
    Container: {
      width: 24,
      height: 24,
      display: 'inline-flex',
      alignItems: 'center'
    }
  }
};
