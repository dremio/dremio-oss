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
import { PureComponent, Component } from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import Immutable from 'immutable';
import { withRouter } from 'react-router';
import { connect } from 'react-redux';
import { isWikAvailable } from '@app/selectors/explore';
import { PageTypes, pageTypesProp } from '@app/pages/ExplorePage/pageTypes';
import { changePageTypeInUrl } from '@app/pages/ExplorePage/pageTypeUtils';
import Art from '@app/components/Art';
import { formatMessage } from '@app/utils/locale';
import PageTypeButtonsMixin from 'dyn-load/pages/ExplorePage/components/PageTypeButtonsMixin';
import { buttonsContainer, button, buttonActive, icon as iconClass } from './PageTypeButtons.less';

class PageTypeButtonView extends Component {
  static propTypes = {
    text: PropTypes.string,
    icon: PropTypes.string,
    isSelected: PropTypes.bool,
    onClick: PropTypes.func,
    dataQa: PropTypes.string
  };

  render() {
    const {
      isSelected,
      onClick,
      text,
      icon,
      dataQa
    } = this.props;

    return <span
      className={classNames(button, isSelected && buttonActive)}
      onClick={onClick}
      data-qa={dataQa}>
      {icon && <Art src={`${icon}.svg`} className={iconClass} alt={text} />}
      {text}
    </span>;
  }
}

class ButtonController extends PureComponent {
  static propTypes = {
    selectedPageType: pageTypesProp,
    pageType: pageTypesProp.isRequired,
    text: PropTypes.string,
    icon: PropTypes.string,
    dataQa: PropTypes.string,

    //withRouter props
    location: PropTypes.object.isRequired,
    router: PropTypes.object.isRequired
  };

  setPageType = () => {
    const {
      selectedPageType,
      pageType, // new page type to select
      location,
      router
    } = this.props;

    if (selectedPageType !== pageType) {
      const pathname = changePageTypeInUrl(location.pathname, pageType);
      router.push({...location, pathname});
    }
  };

  render() {
    const {
      selectedPageType,
      pageType,
      text,
      icon,
      dataQa
    } = this.props;

    return <PageTypeButtonView
      isSelected={selectedPageType === pageType}
      text={text}
      icon={icon}
      onClick={this.setPageType}
      dataQa={dataQa}
      />;
  }
}

const PageTypeButton = withRouter(ButtonController);

const buttonsConfigs = {
  [PageTypes.default]: {
    intlId: 'Dataset.Data',
    icon: 'Data',
    dataQa: 'Data'
  },
  [PageTypes.wiki]: {
    intlId: 'Dataset.Wiki',
    icon: 'Wiki',
    dataQa: 'Wiki'
  },
  [PageTypes.graph]: {
    intlId: 'Dataset.Graph',
    icon: 'DataGraph',
    dataQa: 'Graph'
  }
};

const mapStateToProps = (state, { location }) => ({
  showWiki: isWikAvailable(state, location)
});

@PageTypeButtonsMixin
export class PageTypeButtonsView extends PureComponent {
  static propTypes = {
    selectedPageType: pageTypesProp,
    dataset: PropTypes.instanceOf(Immutable.Map),
    showWiki: PropTypes.bool,
    dataQa: PropTypes.string
  };

  getAvailablePageTypes() {
    const {
      showWiki
    } = this.props;
    const pageTypeList = [PageTypes.default];

    if (showWiki) {
      pageTypeList.push(PageTypes.wiki);
    }

    return pageTypeList;
  }

  render() {
    const {
      selectedPageType,
      dataQa
    } = this.props;
    const pagetTypes = this.getAvailablePageTypes();

    return <span className={buttonsContainer}
      data-qa={dataQa}>
      {pagetTypes.map(pageType => {
        const {
          intlId,
          ...rest
        } = buttonsConfigs[pageType];

        return <PageTypeButton key={pageType}
          selectedPageType={selectedPageType}
          text={formatMessage(intlId)}
          pageType={pageType}
          {...rest}
          />;
      })}
    </span>;
  }
}

export const PageTypeButtons = withRouter(connect(mapStateToProps)(PageTypeButtonsView));
