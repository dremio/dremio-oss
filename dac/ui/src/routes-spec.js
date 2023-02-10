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
import { mount } from "enzyme";

import { Router, Route, createMemoryHistory } from "react-router";
import { EntityLink } from "@app/pages/HomePage/components/EntityLink";
import * as commonPaths from "dremio-ui-common/paths/common.js";

describe("routes", () => {
  const linkUrl = commonPaths.source.link({ resourceId: "fake_source_id" });
  const linkProps = {
    linkTo: linkUrl,
    activeClassName: "active",
  };
  const renderLink = () => (
    <EntityLink {...linkProps} className="finder-nav-item-link" />
  );
  const isLinkActive = (wrapper) =>
    wrapper.find(".finder-nav-item-link").hostNodes().hasClass("active");

  describe("NavLinkItem highlight tests", () => {
    it("link is marked as active when a source is selected", () => {
      const wrapper = mount(
        <Router history={createMemoryHistory(linkUrl)}>
          <Route
            path={commonPaths.projectBase.fullRoute()}
            component={({ children }) => children}
          >
            <Route path={commonPaths.source.fullRoute()} component={renderLink}>
              <Route path={commonPaths.sourceFolder.fullRoute()} />
            </Route>
          </Route>
        </Router>
      );

      expect(isLinkActive(wrapper)).to.equal(true);
    });

    it("link is marked as active when a subfolder of a source is selected", () => {
      const wrapper = mount(
        <Router
          history={createMemoryHistory(`${linkUrl}/folder/fake_subfolder`)}
        >
          <Route
            path={commonPaths.projectBase.fullRoute()}
            component={({ children }) => children}
          >
            <Route path={commonPaths.source.fullRoute()} component={renderLink}>
              <Route path={commonPaths.sourceFolder.fullRoute()} />
            </Route>
          </Route>
        </Router>
      );

      expect(isLinkActive(wrapper)).to.equal(true);
    });
  });
});
