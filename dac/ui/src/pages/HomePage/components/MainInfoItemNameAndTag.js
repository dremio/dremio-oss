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
import { get } from 'lodash/object';
import { startSearch as startSearchAction } from 'actions/search';
import TagsModal from 'pages/HomePage/components/modals/TagsModal/TagsModal';
import { Tag } from '@app/pages/ExplorePage/components/TagsEditor/Tag';
import MainInfoItemName from './MainInfoItemName';

import { tag as tagClass} from './MainInfoItemNameAndTag.less';

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
    this.cell = null;
  }

  componentDidMount() {
    const cellWidth = get(this.cell, 'clientWidth');
    if (cellWidth) {
      this.setState({cellWidth}); //eslint-disable-line
    }
  }

  setCellRef = (element) => {
    this.cell = element;
  };

  setChildWidth = (width) => {
    this.setState({nameWidth: width});
  };

  openModal(event) {
    this.setState({modalIsOpen: true, anchorEl: event.currentTarget});
  }
  closeModal() {
    this.setState({modalIsOpen: false});
  }

  onTagClick = (tag) => {
    this.props.startSearch(tag);
  };

  getNamePartWidth = (width) => {
    this.setState({namePartWidth: width});
  };

  getShownTags = tags => {
    //These constants are used only in this method and are mostly based on tagClass css
    const [
      MAX_SHOWN_TAG_CHARS, TAGS_X_PADDING_PX, DEFAULT_TAGS_WIDTH_PX, PX_PER_CHAR, TAG_PADDING_PX, MIN_TAG_WIDTH_PX
    ] = [
      20, 20, 800, 6, 25, 35
    ];

    // show tags which fit in name table cell after name value, max 20 chars per tag
    const tagsWidthInPx = (this.state.cellWidth - this.state.namePartWidth - TAGS_X_PADDING_PX) || DEFAULT_TAGS_WIDTH_PX;
    return tags.reduce((accumulator, tag, index) => {
      const shownTagChars = Math.min(MAX_SHOWN_TAG_CHARS, tag.length);
      //approx 6 px per char + 25px for surrounding
      const shownTagWidth = shownTagChars * PX_PER_CHAR + TAG_PADDING_PX;
      //unless this is the last tag, allow for at least MIN_TAG_WIDTH_PX for the next one
      const remainingWidth = (index < tags.length - 1) ?
        tagsWidthInPx - accumulator.len - MIN_TAG_WIDTH_PX : tagsWidthInPx - accumulator.len;
      if (shownTagWidth <= remainingWidth) {
        accumulator.len += shownTagWidth;
        accumulator.tags.push(tag);
      } else {
        accumulator.extra.push(tag);
      }
      return accumulator;
    }, /* accumulator */{tags: [], extra: [], len: 0});
  };

  renderTag = (tag, index) => {
    return <Tag key={index}
      className={tagClass}
      onClick={() => this.onTagClick(tag)}
      text={tag}
      title />;
  };

  renderTags(item) {
    // Only dataset items may have tags, but we can also double check the item type if needed
    const tagsFromItem = item.get('tags');
    if (!tagsFromItem || !tagsFromItem.size) return '';

    const tags = tagsFromItem.toJS();
    const shownTags = this.getShownTags(tags);
    const selectedTags = shownTags.tags;
    const overflowTags = shownTags.extra;
    return (
      <div>
        {selectedTags.map(this.renderTag)}
        {(tags.length > selectedTags.length) &&
          [
            <Tag key='moreTags' text='...' className={tagClass} onClick={this.openModal} />,
            <TagsModal key='tagsModal'
              isOpen={this.state.modalIsOpen}
              hide={this.closeModal}
              tags={overflowTags}
              anchorEl={this.state.anchorEl}
              onTagClick={this.onTagClick}
            />
          ]
        }
      </div>
    );
  }

  render() {
    const { item } = this.props;
    return (
      <div style={{display: 'flex'}} ref={this.setCellRef}>
        <MainInfoItemName item={item} onMount={this.getNamePartWidth}/>
        {this.renderTags(item)}
      </div>
    );
  }

}

export default connect(null, dispatch => ({
  startSearch: startSearchAction(dispatch)
}))(MainInfoItemNameAndTag);
