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
import { PureComponent } from 'react';
import { connect } from 'react-redux';
import classNames from 'classnames';
import {FormattedMessage, injectIntl} from 'react-intl';
import Immutable from 'immutable';

import PropTypes from 'prop-types';

import FinderNav from 'components/FinderNav';
import FinderNavItem from 'components/FinderNavItem';
import ViewStateWrapper from 'components/ViewStateWrapper';
import LinkButton from 'components/Buttons/LinkButton';
import SimpleButton from 'components/Buttons/SimpleButton';
import { EmptyStateContainer } from '@app/pages/HomePage/components/EmptyStateContainer';
import SpacesSection from '@app/pages/HomePage/components/SpacesSection';

import { isExternalSourceType } from '@app/constants/sourceTypes';
import { PALE_NAVY } from 'uiTheme/radium/colors';

import ApiUtils from 'utils/apiUtils/apiUtils';
import {createSampleSource, isSampleSource} from 'actions/resources/sources';
import {toggleExternalSourcesExpanded} from 'actions/ui/ui';

import Radium from 'radium';

import { emptyContainer, shortEmptyContainer } from './LeftTree.less';

@injectIntl
@Radium
export class LeftTree extends PureComponent {
  static contextTypes = {
    location: PropTypes.object,
    loggedInUser: PropTypes.object,
    router: PropTypes.object
  };

  static propTypes = {
    className: PropTypes.string,
    sources: PropTypes.instanceOf(Immutable.List).isRequired,
    sourceTypesIncludeS3: PropTypes.bool,
    sourcesViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    createSampleSource: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired,
    externalSourcesExpanded: PropTypes.bool,
    toggleExternalSourcesExpanded: PropTypes.func
  };

  state = {
    isAddingSampleSource: false
  };

  addSampleSource = () => {
    this.setState({isAddingSampleSource: true});
    return this.props.createSampleSource().then((response) => {
      if (response && !response.error) {
        const nextSource = ApiUtils.getEntityFromResponse('source', response);
        this.context.router.push(nextSource.getIn(['links', 'self']));
      }
      this.setState({isAddingSampleSource: false});
    });
  };

  getHomeObject() {
    return {
      name: this.context.loggedInUser.userName,
      links: {
        self: '/home'
      },
      resourcePath: '/home',
      iconClass: 'Home'
    };
  }

  getInitialSourcesContent(sources, isExternalSourceList) {
    const addHref = this.getAddSourceHref(isExternalSourceList);
    const count = sources.size;

    const isEmpty = count === 0;
    const haveOnlySampleSource = isExternalSourceList ? false : !isEmpty && sources.toJS().every(isSampleSource);
    const haveNonSampleSource = isExternalSourceList ? !isEmpty : sources.toJS().some(e => !isSampleSource(e));

    // situations...

    // - only sample source(s), user can't add: show nothing
    if (haveOnlySampleSource && !this.getCanAddSource()) return null;

    // - have a non-sample source: show nothing
    if (haveNonSampleSource) return null;

    // - no sources, user can add: show text and both buttons
    // - no sources, user can't add: show text
    // - only sample source(s), user can add: show text and add button
    return <EmptyStateContainer
      className={isExternalSourceList ? shortEmptyContainer : emptyContainer}
      title={isEmpty ? <FormattedMessage id={isExternalSourceList ? 'Source.NoExternalSources' : 'Source.NoDataLakes'}/>
        : <FormattedMessage id='Source.AddOwnSource'/>}>
      {this.getCanAddSource() && isEmpty && !isExternalSourceList && this.props.sourceTypesIncludeS3 && <SimpleButton
        buttonStyle='primary'
        submitting={this.state.isAddingSampleSource}
        style={{padding: '0 12px'}}
        onClick={this.addSampleSource}>
        <FormattedMessage id='Source.AddSampleSource'/>
      </SimpleButton>}
      {this.getCanAddSource() && <LinkButton
        buttonStyle='primary'
        data-qa={'add-sources'}
        to={addHref}>
        <FormattedMessage id={isExternalSourceList ? 'Source.AddExternalSource' : 'Source.AddDataLake'}/>
      </LinkButton>}
    </EmptyStateContainer>;
  }

  getCanAddSource() {
    return this.context.loggedInUser.admin;
  }

  getAddSourceHref(isExternalSource) {
    return this.getCanAddSource() ?
      {...this.context.location, state: {modal: 'AddSourceModal', isExternalSource}} : undefined;
  }

  render() {
    const { className, sources, sourcesViewState, intl, externalSourcesExpanded, toggleExternalSourcesExpanded: handleToggle } = this.props;
    const classes = classNames('left-tree', className);
    const homeItem = this.getHomeObject();
    const dataLakeSources = sources.filter(source => !isExternalSourceType(source.get('type')));
    const externalSources = sources.filter(source => isExternalSourceType([source.get('type')]));
    return (
      <div className={classes} style={[styles.leftTreeHolder]}>
        <h3 style={[styles.headerViewer]}>
          <FormattedMessage id='Dataset.Datasets'/>
        </h3>
        <ul className='header-block' style={styles.homeWrapper}>
          <FinderNavItem item={homeItem} />
        </ul>
        <SpacesSection />
        <div className='left-tree-wrap' style={{
          minHeight: '175px',
          maxHeight: 'calc(100vh - 430px)'
        }}>
          <ViewStateWrapper viewState={sourcesViewState}>
            <FinderNav
              navItems={dataLakeSources}
              title={intl.formatMessage({ id: 'Source.DataLakes' })}
              addTooltip={intl.formatMessage({ id: 'Source.AddDataLake' })}
              isInProgress={sourcesViewState.get('isInProgress')}
              addHref={this.getAddSourceHref(false)}
              listHref='/sources/datalake/list'
              children={this.getInitialSourcesContent(dataLakeSources, false)}
            />
          </ViewStateWrapper>
        </div>
        <div className='left-tree-wrap' style={{
          minHeight: '150px',
          maxHeight: 'calc(100vh - 455px)'
        }}>
          <ViewStateWrapper viewState={sourcesViewState}>
            <FinderNav
              navItems={externalSources}
              title={intl.formatMessage({ id: 'Source.ExternalSources' })}
              addTooltip={intl.formatMessage({ id: 'Source.AddExternalSource' })}
              isInProgress={sourcesViewState.get('isInProgress')}
              addHref={this.getAddSourceHref(true)}
              isCollapsible
              isCollapsed={!externalSourcesExpanded}
              onToggle={handleToggle}
              listHref='/sources/external/list'
              children={this.getInitialSourcesContent(externalSources, true)}
            />
          </ViewStateWrapper>
        </div>
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    externalSourcesExpanded: state.ui.get('externalSourcesExpanded')
  };
}

export default connect(mapStateToProps, {createSampleSource, toggleExternalSourcesExpanded})(LeftTree);

const styles = {
  leftTreeHolder: {
    width: '250px',
    flexShrink: '0',
    overflow: 'auto',
    borderRight: '1px solid rgba(0,0,0,.1)',
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
    maxHeight: '100%'
  },
  homeWrapper: {
    flex: '0 0 auto',

    // this lines up the separator lines between the two panels:
    // (actually it's slightly off, but due to color differences/gestalt this actually looks more correct)
    paddingTop: 5,
    paddingBottom: 5
  },
  hItem: {
    display: 'block',
    width: '230px',
    height: '28px',
    padding: '2px 0',
    position: 'relative',
    cursor: 'pointer',
    borderRadius: '2px',
    ':hover': {
      background: '#fff5dc'
    }
  },
  userText: {
    margin: '5px 0 0 6px',
    display: 'inline-block'
  },
  fontIcon: {
    Container: {
      verticalAlign: 'top'
    }
  },
  headerViewer: {
    display: 'flex',
    alignItems: 'center',
    background: PALE_NAVY,
    height: 38,
    flex: '0 0 auto',
    padding: '0 10px'
  },
  columnFlex: { // we need this in a number of places to keep the DOM tree in flex mode
    flex: '2',
    minHeight: '25vh'
  }
};
