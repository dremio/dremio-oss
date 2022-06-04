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
import exploreUtils from '@app/utils/explore/exploreUtils';
import { buttonsContainer, button, buttonActive, icon as iconClass } from './PageTypeButtons.less';

export class SinglePageTypeButton extends Component {
  static propTypes = {
    text: PropTypes.string,
    icon: PropTypes.string,
    isSelected: PropTypes.bool,
    onClick: PropTypes.func,
    dataQa: PropTypes.string,
    classname: PropTypes.string
  };

  render() {
    const {
      isSelected,
      onClick,
      text,
      icon,
      dataQa,
      classname
    } = this.props;

    return <span
      className={classNames(button, isSelected && buttonActive, classname)}
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

    return <SinglePageTypeButton
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
    icon: 'HeaderData',
    dataQa: 'Data'
  },
  [PageTypes.wiki]: {
    intlId: 'Dataset.Wiki',
    icon: 'HeaderMetadata',
    dataQa: 'Wiki'
  },
  [PageTypes.graph]: {
    intlId: 'Dataset.Graph',
    icon: 'HeaderGraph',
    dataQa: 'Graph'
  },
  [PageTypes.reflections]: {
    intlId: 'Reflection.Reflections',
    icon: 'HeaderReflection',
    dataQa: 'Reflections'
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
    dataQa: PropTypes.string,
    location: PropTypes.object
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
      dataQa,
      location
    } = this.props;
    const pageTypes = this.getAvailablePageTypes();
    const isDatasetPage = exploreUtils.isExploreDatasetPage(location);

    // Show tabbed content for dataset
    if (pageTypes.length > 1 && isDatasetPage) {
      return <span className={buttonsContainer}
        data-qa={dataQa}>
        {pageTypes.map(pageType => {
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
    } else {
      // Hide all tabs for New Query and Updated Queries
      return null;
    }
  }
}

export const PageTypeButtons = withRouter(connect(mapStateToProps)(PageTypeButtonsView));
