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

import { cloneDeep } from "lodash";
import addAlwaysPresent, {
  crossSourceSelectionUiConfig,
  getAlwaysPresentFunctionalConfig,
  inlineMetadataRefreshConfig,
} from "./globalSourceConfigUtil";

describe("globalSourceConfigUtil", () => {
  const defaultFunctionalConfig = {
    elements: [],
  };

  const defaultUiConfig = {
    form: {
      tabs: [
        {
          name: "General",
        },
        {
          name: "Advanced Options",
          sections: [
            {
              elements: [
                {
                  propName: "config.allowCreateDrop",
                },
              ],
            },
          ],
        },
      ],
    },
  };

  it("adds always present config to functional config and ui config", () => {
    const functionalConfig = cloneDeep(defaultFunctionalConfig);
    const uiConfig = cloneDeep(defaultUiConfig);

    const expectedFunctionalConfig = {
      ...functionalConfig,
      elements: [
        ...functionalConfig.elements,
        ...getAlwaysPresentFunctionalConfig(),
      ],
    };

    const expectedUiConfig = cloneDeep(uiConfig);
    expectedUiConfig.form.tabs[1].sections[0].elements.push(
      crossSourceSelectionUiConfig,
      inlineMetadataRefreshConfig,
    );
    addAlwaysPresent(functionalConfig, uiConfig);
    expect(functionalConfig).to.deep.equal(expectedFunctionalConfig);
    expect(uiConfig).to.deep.equal(expectedUiConfig);
  });

  it("handles case where there are no elements in advanced tab", () => {
    const functionalConfig = cloneDeep(defaultFunctionalConfig);
    const uiConfig = {
      form: {
        tabs: [
          {
            name: "General",
          },
          {
            name: "Advanced Options",
          },
        ],
      },
    };

    const expectedFunctionalConfig = {
      ...functionalConfig,
      elements: [
        ...functionalConfig.elements,
        ...getAlwaysPresentFunctionalConfig(),
      ],
    };

    const expectedUiConfig = cloneDeep(uiConfig);
    expectedUiConfig.form.tabs[1].sections = [
      {
        elements: [crossSourceSelectionUiConfig, inlineMetadataRefreshConfig],
      },
    ];
    addAlwaysPresent(functionalConfig, uiConfig);
    expect(functionalConfig).to.deep.equal(expectedFunctionalConfig);
    expect(uiConfig).to.deep.equal(expectedUiConfig);
  });
});
