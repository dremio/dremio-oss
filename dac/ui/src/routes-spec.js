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
import { mount } from 'enzyme';

import { Router, Route, createMemoryHistory } from 'react-router';
import { getSourceRoute } from 'routes';
import FinderNavItem from 'components/FinderNavItem';

describe('routes', () => {

  const rootType = 'source';
  const linkUrl = `/${rootType}/fake_source_id`;
  const navLinkProps = {
    item: {
      links: {
        self: linkUrl
      },
      state: { // needed for rendering
      }
    }
  };
  const renderLink = () => <FinderNavItem {...navLinkProps} />;
  let history;

  beforeEach(() => {
    history = createMemoryHistory();
  });

  describe('NavLinkItem highlight tests', () => {
    it('link is marked as active when a source is selected', () => {
      const wrapper = mount(
        <Router history={history}>
          <Route path='/' component={({ children }) => children}>
            {getSourceRoute(rootType, renderLink)}
          </Route>
        </Router>
        );

      history.push(linkUrl);

      expect(wrapper.find('.finder-nav-item-link').hasClass('active')).to.equal(true);
    });

    it('link is marked as active when a subfolder of a source is selected', () => {
      const wrapper = mount(
        <Router history={history}>
          <Route path='/' component={({ children }) => children}>
            {getSourceRoute(rootType, renderLink)}
          </Route>
        </Router>
        );

      history.push(`${linkUrl}/folder/fake_subfolder`);

      expect(wrapper.find('.finder-nav-item-link').hasClass('active')).to.equal(true);
    });

    it('link is inactive when other path is selected', () => {
      const otherRootName = 'not_a_' + rootType;
      const wrapper = mount(
        <Router history={history}>
          <Route path='/' component={({ children }) => children}>
            {getSourceRoute(otherRootName, renderLink)}
          </Route>
        </Router>
        );

      history.push(`${otherRootName}/some_id/folder/fake_subfolder`);

      expect(wrapper.find('.finder-nav-item-link').hasClass('active')).to.equal(false);
    });
  });

});
