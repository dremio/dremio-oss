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
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import Immutable from 'immutable';
import { FormattedMessage, injectIntl } from 'react-intl';
import { withRouter } from 'react-router';
import { compose } from 'redux';

import LinkButton from '@app/components/Buttons/LinkButton';
import { EmptyStateContainer } from '@app/pages/HomePage/components/EmptyStateContainer';
import FinderNav from '@app/components/FinderNav';
import SpacesLoader from '@app/pages/HomePage/components/SpacesLoader';
import ViewStateWrapper, { viewStatePropType } from '@app/components/ViewStateWrapper';
import { ALL_SPACES_VIEW_ID } from '@app/actions/resources/spaces';
import { getViewState} from '@app/selectors/resources';
import { getSortedSpaces } from '@app/selectors/home';
import { FLEX_COL_START } from '@app/uiTheme/radium/flexStyle';
import { SpacesSectionMixin, mapStateToProps as mixinMapStateToProps } from 'dyn-load/pages/HomePage/components/SpacesSectionMixin';

import { emptyContainer } from './LeftTree.less';

const mapStateToProps = state => ({
  spaces: getSortedSpaces(state),
  spacesViewState: getViewState(state, ALL_SPACES_VIEW_ID),
  ...(mixinMapStateToProps ? mixinMapStateToProps(state) : null)
});

@SpacesSectionMixin
export class SpacesSection extends PureComponent {
  static propTypes = {
    //#region react-redux

    spaces: PropTypes.instanceOf(Immutable.List).isRequired,
    spacesViewState: viewStatePropType,

    //#endregion

    // injectIntl
    intl: PropTypes.object.isRequired,

    // withRouter
    location: PropTypes.object.isRequired
  }

  getAddSpaceHref() {
    return { ...this.props.location, state: { modal: 'SpaceModal' } };
  }

  getInitialSpacesContent() {
    const addHref = this.getAddSpaceHref();
    return this.props.spaces.size === 0 ? <EmptyStateContainer
      className={emptyContainer}
      title={<FormattedMessage id='Space.NoSpaces'/>}>
      {addHref && <LinkButton
        buttonStyle='primary'
        data-qa={'add-spaces'}
        to={addHref}>
        <FormattedMessage id='Space.AddSpace'/>
      </LinkButton>}
    </EmptyStateContainer> : null;
  }

  render() {
    const {
      spacesViewState,
      spaces,
      intl
    } = this.props;

    return (
      <div className='left-tree-wrap' style={{
        minHeight: '165px',
        overflow: 'hidden',
        flex: 1
      }}>
        <SpacesLoader />
        <ViewStateWrapper viewState={spacesViewState} style={FLEX_COL_START}>
          <FinderNav
            navItems={spaces}
            title={intl.formatMessage({ id: 'Space.Spaces' })}
            addTooltip={intl.formatMessage({ id: 'Space.AddSpace'})}
            isInProgress={spacesViewState.get('isInProgress')}
            addHref={this.getAddSpaceHref()}
            listHref='/spaces/list'
            children={this.getInitialSpacesContent()}
          />
        </ViewStateWrapper>
      </div>
    );
  }
}



export default compose(
  connect(mapStateToProps),
  withRouter,
  injectIntl
)(SpacesSection);
