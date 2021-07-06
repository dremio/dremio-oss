/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { PureComponent, Component } from 'react';
import classNames from 'classnames';
import PropTypes from 'prop-types';
import Art from '@app/components/Art';
import SimpleButton from '@app/components/Buttons/SimpleButton';
import {
  notificationIcon,
  wikiButton as wikiButtonCls,
  wikiButtonSelected
} from './WikiButton.less';

// Commenting this out for now for https://dremio.atlassian.net/browse/DX-31621
// const mapStateToProps = (state) => {
//   return {
//     hasWiki: isWikiPresent(state)
//   };
// };

export class WikiButtonView extends PureComponent {
  static propTypes = {
    isSelected: PropTypes.bool, // toggle state
    onClick: PropTypes.func,
    //this property is connected to redux store
    showNotification: PropTypes.bool, // true to show an orange notification circle.
    className: PropTypes.string
  };

  render() {
    const {
      onClick,
      showNotification,
      isSelected,
      className
    } = this.props;

    return <span className={classNames(wikiButtonCls, isSelected && wikiButtonSelected, className)}>
      <SimpleButton key='button' style={{minWidth: 0, marginRight: 0, outline: 'none'}}
        buttonStyle={isSelected ? 'primary' : 'secondary'}
        onClick={onClick}
        title={la('Wiki')}>
        <Art key='icon' src={isSelected ? 'SidebarActive.svg' : 'Sidebar.svg'} alt='' style={{height: 24}} />
        {showNotification && <div key='notification' className={notificationIcon}></div>}
      </SimpleButton>
    </span>;
  }
}

// @connect(mapStateToProps)
export class WikiButton extends Component {
  static propTypes = {
    isSelected: PropTypes.bool, // toggle state
    onClick: PropTypes.func,
    className: PropTypes.string
  };
  render() {
    const {
      isSelected,
      onClick,
      className
    } = this.props;

    const props = {
      isSelected,
      onClick,
      className
    };

    return <WikiButtonView {...props} showNotification={false} />;
  }
}
