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
import TagsModal from 'pages/HomePage/components/modals/TagsModal/TagsModal';
import { Tag } from '@app/pages/ExplorePage/components/TagsEditor/Tag';
import ImmutablePropTypes from 'react-immutable-proptypes';

import { tag as tagClass} from './TagList.less';

export class TagListView extends Component {
  static propTypes = {
    tags: ImmutablePropTypes.listOf(PropTypes.string).isRequired,
    visibleTagsNumber: PropTypes.number,
    onTagClick: PropTypes.func, // (tagName) => void
    className: PropTypes.string,
    style: PropTypes.object,
    onMainContainerRef: PropTypes.func
  };

  render() {
    const {
      tags,
      visibleTagsNumber,
      onTagClick,
      className,
      style,
      onMainContainerRef
    } = this.props;

    const showModal = tags.size > visibleTagsNumber;
    const visible = showModal ? tags.slice(0, visibleTagsNumber) : tags;
    const modalTags = showModal ? tags.slice(visibleTagsNumber) : null;
    return (
      <div ref={onMainContainerRef} className={className} style={style}>
        {
          visible.map(tag => <Tag key={tag}
            onClick={onTagClick ? () => onTagClick(tag) : null}
            className={tagClass}
            text={tag}
            title
          />)
        }
        {
          showModal && <TagsModal key='tagsModal'
            tags={modalTags.toJS()}
            onTagClick={onTagClick}
            mainTagClass={tagClass}
          />
        }
      </div>
    );
  }
}

// supports calculation of visible tags count depending on component width.
export class TagList extends Component {
  static propTypes = {
    tags: ImmutablePropTypes.listOf(PropTypes.string).isRequired,
    onTagClick: PropTypes.func, // (tagName) => void
    className: PropTypes.string,
    style: PropTypes.object
  };

  state = {
    el: { clientWidth : null }
  };

  onRef = (el) => {
    this.setState({
      el
    });
  }

  countVisibleTags = () => {
    const { tags } = this.props;
    //These constants are used only in this method and are mostly based on tagClass css
    const MAX_TAG_WIDTH = 100,
      DEFAULT_TAGS_WIDTH_PX = 800,
      PX_PER_CHAR = 6,
      TAG_PADDING_PX = 22,
      MIN_TAG_WIDTH_PX = 35; // width for '...' button

    let remainingWidth = this.state.el.clientWidth || DEFAULT_TAGS_WIDTH_PX;

    const totalCount = tags.size;
    let i;

    for (i = 0; i < totalCount; i++) {
      const tag = tags.get(i);
      const currentTagWidth = Math.min(tag.length * PX_PER_CHAR + TAG_PADDING_PX, MAX_TAG_WIDTH);

      if (currentTagWidth > remainingWidth) { // no more space
        if (remainingWidth >= MIN_TAG_WIDTH_PX) { //there is enough space to show '...'
          return i;
        }
        // not enough space for '...' button, so do not include current tag in result. I assume that
        // current tag has width more than '...' button
        return i === 0 ? 0 : i - 1;
      }

      remainingWidth -= currentTagWidth;
    }

    // all tags could be displayed
    return totalCount;
  };

  render() {
    return (
      <TagListView
        onMainContainerRef={this.onRef}
        {...this.props}
        visibleTagsNumber={this.countVisibleTags()}
      />
    );
  }
}
