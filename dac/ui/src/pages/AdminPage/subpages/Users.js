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
import Immutable from 'immutable';
import { connect }   from 'react-redux';
import shallowEqual from 'shallowequal';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { USERS_VIEW_ID, searchUsers, removeUser } from 'actions/admin';
import { showConfirmationDialog } from 'actions/confirmation';
import { getUsers } from 'selectors/admin';
import { getViewState } from 'selectors/resources';

import UsersView from './UsersView';


@pureRender
export class Users extends Component {
  static propTypes = {
    location: PropTypes.object,
    searchUsers: PropTypes.func,
    removeUser: PropTypes.func,
    users: PropTypes.instanceOf(Immutable.List),
    viewState: PropTypes.instanceOf(Immutable.Map),
    showConfirmationDialog: PropTypes.func
  }

  componentWillMount() {
    this.loadPageData(this.props.location);
  }

  componentWillReceiveProps(nextProps) {
    if (!shallowEqual(this.props.location.query, nextProps.location.query)
      || nextProps.viewState.get('invalidated')
    ) {
      this.loadPageData(nextProps.location);
    }
  }

  loadPageData(location) { // todo: throttle this. also do we need to deal with cancelling a previous search?
    const { filter } = location.query || {};
    this.props.searchUsers(filter);
    // paging not yet implimented:
    //const { filter, pageNumber, itemsOnPage } = location.query || {};
    //const itemsOnPageValid = parseInt(itemsOnPage, 10) || 10;
    //const currentPageNumberValid = parseInt(pageNumber, 10) || 1;
    // this.props.searchUsers(filter, currentPageNumberValid, itemsOnPageValid);
  }

  removeUser = (user) => {
    this.props.removeUser(user).then(() => {
      this.props.searchUsers();
    });
  }

  handleRemoveUser = (user) => {
    this.props.showConfirmationDialog({
      title: la('Remove User'),
      text: la('Are you sure you want to remove this user?'),
      confirmText: la('Remove'),
      confirm: () => this.removeUser(user)
    });
  }

  search = (e) => {
    const value = e.target.value;
    this.props.searchUsers(value);
  }

  render() {
    return (
      <UsersView
        viewState={this.props.viewState}
        users={this.props.users}
        removeUser={this.handleRemoveUser}
        search={this.search}
      />
    );
  }
}

function mapStateToProps(state) {
  return {
    users: getUsers(state),
    viewState: getViewState(state, USERS_VIEW_ID)
  };
}

export default connect(mapStateToProps, {
  searchUsers,
  removeUser,
  showConfirmationDialog
})(Users);
