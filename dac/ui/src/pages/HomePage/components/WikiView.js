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
import { connect } from 'react-redux';
import { fromJS } from 'immutable';
import { injectIntl } from 'react-intl';
import { MarkdownEditorView } from 'components/MarkdownEditor';
import ViewStateWrapper from 'components/ViewStateWrapper';
import { SectionTitle, getIconButtonConfig } from '@app/pages/ExplorePage/components/Wiki/SectionTitle';
import { WikiEmptyState } from '@app/components/WikiEmptyState';
import { WikiModal } from '@app/pages/ExplorePage/components/Wiki/WikiModal';
import { wikiSaved as wikiSavedAction } from '@app/actions/home';
import {
  getWikiValue,
  getWikiVersion,
  isWikiLoading,
  getErrorInfo
} from '@app/selectors/home';
import {
  wikiWrapper,
  wikiWidget,
  emptyContainer,
  editor,
  wikiTitle
} from './WikiView.less';

const ViewModes = ['default', 'edit', 'expanded'].reduce((enumResult, value) => {
  enumResult[value] = value;
  return enumResult;
}, {});

const getEntityId = (item) => {
  return item && item.get('id');
};

const getCanEditWiki = (item) => {
  // can edit if entity has no permissions (true for home space and in CE) or explicit canEdit permission is present
  return item ? item.getIn(['permissions', 'canEdit'], true) : false;
};

const mapStateToProps = (state, /* ownProps */ { item }) => {
  const entityId = getEntityId(item);
  const error = getErrorInfo(state, entityId);
  return {
    wikiText: getWikiValue(state, entityId) || '',
    wikiVersion: getWikiVersion(state, entityId),
    entityId,
    canEditWiki: getCanEditWiki(item),
    isLoading: isWikiLoading(state, entityId),
    errorMessage: error ? error.message : null,
    errorId: error ? error.id : null
  };
};

@injectIntl
@connect(mapStateToProps, {
  wikiSaved: wikiSavedAction
})
export default class WikiView extends Component {

  static propTypes = {
    intl: PropTypes.object.isRequired,

    // connected
    entityId: PropTypes.string,
    wikiText: PropTypes.string.isRequired,
    wikiVersion: PropTypes.number,
    canEditWiki: PropTypes.bool.isRequired,
    isLoading: PropTypes.bool,
    errorMessage: PropTypes.string,
    errorId: PropTypes.string,
    wikiSaved: PropTypes.func // (entityId, text, version) => void
  };

  state = {
    viewMode: ViewModes.default
  };

  editWiki = () => {
    this.setState({
      viewMode: ViewModes.edit
    });
  };

  expandWiki = () => {
    this.setState({
      viewMode: ViewModes.expanded
    });
  };


  onSaveWiki = ({
    text,
    version
  }) => {
    const {
      wikiSaved,
      entityId
    } = this.props;

    wikiSaved(entityId, text, version);
    this.setState({
      viewMode: ViewModes.default
    });
  };


  stopEditWiki = () => {
    this.setState({
      viewMode: ViewModes.default
    });
  };

  getTitleButtons() {
    const { canEditWiki, wikiText } = this.props;

    // if there is now wiki we should show only 'Add wiki' button. The other buttons are redundant
    if (!wikiText) return [];

    const buttonList = [getIconButtonConfig({
      key: 'expand',
      icon: 'ExpandWiki',
      dataQa: 'expand-wiki',
      altText: la('expand wiki text'),
      onClick: this.expandWiki
    })];

    if (canEditWiki) {
      buttonList.push(this.getEditButton());
    }
    return buttonList;
  }

  getEditButton() {
    return getIconButtonConfig({
      key: 'edit',
      icon: 'Edit',
      dataQa: 'edit-wiki',
      altText: this.props.intl.formatMessage({id: 'Common.Edit'}),
      onClick: this.editWiki
    });
  }

  getViewState() {
    //todo apply reselect here
    const {
      isLoading,
      errorMessage,
      errorId
    } = this.props;

    if (errorMessage) {
      return fromJS({
        isFailed: true,
        error: {
          message: errorMessage,
          id: errorId
        }
      });
    }
    return fromJS({isInProgress: isLoading});
  }

  render() {
    const {
      viewMode
    } = this.state;

    const {
      wikiText,
      wikiVersion,
      canEditWiki,
      entityId
    } = this.props;

    return (
      <ViewStateWrapper viewState={this.getViewState()}
        hideChildrenWhenFailed={false}
        style={styles.wrapperStylesFix}
      >
        <div className={wikiWrapper} data-qa='wikiWrapper'>
          <SectionTitle
            title={la('Wiki')}
            titleClass={wikiTitle}
            buttons={this.getTitleButtons()}
          />
          {(wikiText.length) ?
            <div className={wikiWidget} data-qa='wikiWidget'>
              <MarkdownEditorView
                value={wikiText}
                className={editor}
                readMode
                fitToContainer
              />
            </div>
            : <WikiEmptyState className={emptyContainer} onAddWiki={canEditWiki ? this.editWiki : null } />
          }
          <WikiModal
            isOpen={viewMode !== ViewModes.default}
            isReadMode={viewMode === ViewModes.expanded}
            topSectionButtons={viewMode === ViewModes.expanded && canEditWiki ? [this.getEditButton()] : null}
            entityId={entityId}
            wikiValue={wikiText}
            wikiVersion={wikiVersion}
            save={this.onSaveWiki}
            cancel={this.stopEditWiki}
          />
        </div>
      </ViewStateWrapper>
    );
  }
}

const styles = {
  icon: {
    height: 16,
    width: 16
  },
  wrapperStylesFix: {
    height: '100%'
  }
};

