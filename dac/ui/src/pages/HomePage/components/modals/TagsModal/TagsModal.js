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
import { injectIntl } from 'react-intl';
import { SelectView } from '@app/components/Fields/SelectView';
import Menu from 'components/Menus/Menu';
import MenuItem from 'components/Menus/MenuItem';
import { Tag } from '@app/pages/ExplorePage/components/TagsEditor/Tag';

import { tag as tagClass, popoverContainer, triangle, triangleHeight } from './TagsModal.less';


@injectIntl
export default class TagsModal extends Component {
  static propTypes = {
    mainTagClass: PropTypes.string,
    onTagClick: PropTypes.func,
    tags: PropTypes.array,
    intl: PropTypes.object.isRequired
  };

  render() {
    const { tags, onTagClick, mainTagClass } = this.props;

    return (
      <SelectView
        content={
          <Tag key='moreTags' text='...' className={mainTagClass} onClick={this.openModal} />
        }
        listStyle={{
          marginTop: parseInt(triangleHeight, 10),
          overflow: 'visible'
        }}
        listRightAligned
        hideExpandIcon
      >
        <div className={triangle}/>
        <div className={popoverContainer} data-qa='tagOverflowPopover'>
          <Menu>
            {tags.map((tag, index) => <MenuItem key={`item${index}`}>
              <Tag key={index}
                className={tagClass}
                onClick={onTagClick ? () => onTagClick(tag) : null}
                text={tag}
                title />
            </MenuItem>)}
          </Menu>
        </div>
      </SelectView>
    );
  }

}
