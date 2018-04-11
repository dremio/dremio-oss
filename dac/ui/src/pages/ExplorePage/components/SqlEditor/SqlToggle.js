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
import { connect } from 'react-redux';
import Immutable from 'immutable';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';
import { FormattedMessage, injectIntl } from 'react-intl';
import PropTypes from 'prop-types';
import Art from 'components/Art';

import { toggleExploreSql } from 'actions/explore/ui';

import { formLabel } from 'uiTheme/radium/typography';

@injectIntl
@pureRender
@Radium
export class SqlToggle extends Component {

  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    style: PropTypes.object,

    //connected
    toggleExploreSql: PropTypes.func.isRequired,
    sqlState: PropTypes.bool,
    intl: PropTypes.object.isRequired
  };

  static contextTypes = {
    location: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired
  };

  handleToggleClick = () => {
    this.props.toggleExploreSql();
  };

  render() {
    const { dataset, sqlState, style, intl } = this.props;
    const canToggle = !dataset.get('isNewQuery');
    const iconType = sqlState ? 'TriangleDown.svg' : 'TriangleRight.svg';
    const iconAlt = intl.formatMessage({ id: `Common.${sqlState ? 'Undisclosed' : 'Disclosed'}` });

    return (
      <div
        className='sql-toggle'
        data-qa='sql-toogle'
        onClick={canToggle ? this.handleToggleClick : undefined}
        style={[styles.base, canToggle && {cursor: 'pointer', marginLeft: sqlState ? 5 : 0}, style]}>
        {canToggle && <Art src={iconType} alt={iconAlt} style={styles.iconStyle}/>}
        <span style={[formLabel]}>
          <FormattedMessage id='SQL.SQLEditor'/>
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
    width: 24,
    height: 24,
    display: 'inline-flex',
    alignItems: 'center'
  }
};
