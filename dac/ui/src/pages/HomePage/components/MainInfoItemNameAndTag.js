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
import { injectIntl } from 'react-intl';
import { connect } from 'react-redux';
import { startSearch as startSearchAction } from 'actions/search';
import { TagList } from '@app/pages/HomePage/components/TagList';

import MainInfoItemName from './MainInfoItemName';

@injectIntl
class MainInfoItemNameAndTag extends Component {

  static propTypes = {
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    intl: PropTypes.object.isRequired,
    startSearch: PropTypes.func // (textToSearch) => {}
  };

  constructor() {
    super();
    this.state = {
      modalIsOpen: false
    };
    this.openModal = this.openModal.bind(this);
    this.closeModal = this.closeModal.bind(this);
  }

  openModal(event) {
    this.setState({modalIsOpen: true, anchorEl: event.currentTarget});
  }
  closeModal() {
    this.setState({modalIsOpen: false});
  }

  onTagClick = (tag) => {
    this.props.startSearch(tag);
  };

  render() {
    const { item } = this.props;
    const tagsFromItem = item.get('tags');
    return (
      <div style={{display: 'flex'}}>
        <MainInfoItemName item={item} />
        {tagsFromItem && tagsFromItem.size && <TagList
          tags={tagsFromItem}
          style={{flex: 1, minWidth: 0}}
          onTagClick={this.onTagClick}
        />}
      </div>
    );
  }
}

export default connect(null, dispatch => ({
  startSearch: startSearchAction(dispatch)
}))(MainInfoItemNameAndTag);
