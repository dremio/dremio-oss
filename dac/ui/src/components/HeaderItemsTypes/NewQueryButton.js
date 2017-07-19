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
import Radium from 'radium';
import { connect } from 'react-redux';

import { showConfirmationDialog } from 'actions/confirmation';
import { resetNewQuery } from 'actions/explore/view';

import { getLocation } from 'selectors/routing';

import { EXPLORE_VIEW_ID } from 'reducers/explore/view';

import FontIcon from 'components/Icon/FontIcon';

import { parseResourceId } from 'utils/pathUtils';

import './NewQueryButton.less';

@Radium
export class NewQueryButton extends Component {

  static propTypes = {
    location: PropTypes.object.isRequired,
    currentSql: PropTypes.string,

    showConfirmationDialog: PropTypes.func,
    resetNewQuery: PropTypes.func
  }

  static contextTypes = {
    username: PropTypes.string.isRequired,
    router: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
  }

  getNewQueryHref() {
    const { location } = this.props;
    const { username } = this.context;
    const resourceId = parseResourceId(location.pathname, username);
    return '/new_query?context=' + encodeURIComponent(resourceId);
  }

  handleClick = () => {
    const { location, currentSql } = this.props;
    if (location.pathname === '/new_query') {
      if (currentSql !== undefined && currentSql.trim()) {
        this.props.showConfirmationDialog({
          title: la('Unsaved Changes Warning'),
          text: [
            la('Performing this action will cause you to lose your current SQL.'),
            la('Are you sure you want to continue?')
          ],
          confirmText: la('Continue'),
          cancelText: la('Cancel'),
          confirm: () => {
            this.props.resetNewQuery(EXPLORE_VIEW_ID);
          }
        });
      } else {
        this.props.resetNewQuery(EXPLORE_VIEW_ID); // even if there's no SQL, clear any errors
      }
    } else {
      this.context.router.push(this.getNewQueryHref());
    }
  }

  render() {
    return (
      <div className='new-query-button' style={styles.base}>
        <a data-qa='new-query-button' onClick={this.handleClick} style={styles.link}>
          <FontIcon theme={styles.icon} type='QueryPlain' />
          {la('New Query')}
        </a>
      </div>
    );
  }
}

function mapStateToProps(state, ownProps) {
  return {
    location: getLocation(state),
    currentSql: state.explore.view.get('currentSql')
  };
}

export default connect(mapStateToProps, {
  showConfirmationDialog,
  resetNewQuery
})(NewQueryButton);


const styles = {
  base: {
    display: 'flex',
    alignItems: 'center'
  },
  link: {
    display: 'block',
    lineHeight: '28px',
    textDecoration: 'none',
    color: '#fff',
    borderRadius: 2,
    border: '1px solid rgba(255, 255, 255, .25)',
    height: 28,
    transition: 'all 0.5s',
    fontSize: '11px',
    padding: '0 10px 0 9px', // chris thinks this looks better
    marginLeft: 10,
    textAlign: 'left',
    cursor: 'pointer'
  },
  icon: {
    'Icon': {
      width: '1.5em',
      height: '1.5em'
    },
    'Container': {
      height: '1.5em',
      display: 'inline-block',
      verticalAlign: '-0.45em',
      marginRight: 9
    }
  }
};
