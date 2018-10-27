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
import { EmptyStateContainer } from '@app/pages/HomePage/components/EmptyStateContainer';
import SimpleButton from '@app/components/Buttons/SimpleButton';

export class WikiEmptyState extends Component {
  static propTypes = {
    onAddWiki: PropTypes.func, // if provided, then "Add Wiki" button is added
    className: PropTypes.string
  };

  render() {
    const {
      onAddWiki,
      className
    } = this.props;
    return (<EmptyStateContainer className={className} title={la('No wiki content')}>
      {onAddWiki && <SimpleButton
        buttonStyle='primary'
        data-qa='wikiCreateBtn'
        style={{padding: '0 12px'}}
        onClick={onAddWiki}>
        {la('Add wiki content')}
      </SimpleButton>}
    </EmptyStateContainer>);
  }
}
