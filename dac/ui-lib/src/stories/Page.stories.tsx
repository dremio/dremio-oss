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
import React from "react";
import { ComponentStory, ComponentMeta } from "@storybook/react";

import { Page } from "../../components";

export default {
  title: "Components/Page",
  component: Page,
} as ComponentMeta<typeof Page>;

const Template: ComponentStory<typeof Page> = (args) => (
  <div style={{ height: "250px" }}>
    <Page
      header={<header className="dremio-page__header">Sample Title</header>}
    >
      <div className="dremio-layout-container">
        <nav className="--fixed">
          <ul>
            <li>Nav item 1</li>
            <li>Nav item 2</li>
            <li>Nav item 3</li>
            <li>Nav item 4</li>
          </ul>
        </nav>
        <div className="dremio-prose">
          <p>
            Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam
            ultricies augue a arcu iaculis, ac maximus massa hendrerit. Mauris
            sed finibus eros, et laoreet nulla. Morbi in magna in justo
            malesuada placerat. Aenean fermentum diam sed orci consectetur
            aliquet. Cras ac ornare risus, ac cursus lacus. Pellentesque
            interdum lacus vitae diam dignissim, a tempus augue eleifend. Morbi
            in dapibus urna, vel viverra mauris.
          </p>

          <p>
            Vivamus aliquam convallis purus nec egestas. In hac habitasse platea
            dictumst. Vivamus placerat id arcu ut feugiat. Interdum et malesuada
            fames ac ante ipsum primis in faucibus. Integer bibendum, eros eget
            faucibus condimentum, quam nunc mattis sem, commodo tempor leo erat
            in ligula. Vestibulum pretium quam velit, nec aliquet arcu tincidunt
            at. Pellentesque sollicitudin imperdiet tortor, ut bibendum neque
            tincidunt sed. Pellentesque blandit risus finibus semper interdum.
          </p>

          <p>
            Mauris sit amet tortor id massa ornare ornare at non dolor. Duis eu
            leo non nunc pretium vestibulum. Nulla vitae diam non sapien
            molestie maximus id nec lectus. Class aptent taciti sociosqu ad
            litora torquent per conubia nostra, per inceptos himenaeos. Nam
            sodales ante et nibh consequat auctor. Mauris sed dictum odio. Etiam
            interdum eget metus ac cursus. Aliquam fringilla pellentesque
            iaculis. Curabitur vel turpis ultrices nunc posuere aliquam eu non
            diam. Sed consectetur gravida enim vitae pharetra. Sed a turpis eget
            ex accumsan feugiat sit amet vitae massa. Nullam mollis at ipsum ac
            iaculis. Curabitur congue tortor at nunc fringilla, nec ultricies
            orci rutrum. Nulla aliquam fermentum sem, ornare convallis libero
            dapibus vel. Vivamus dapibus quam eu enim tempor, sit amet auctor
            mauris consectetur. Praesent in mauris ultrices, luctus ex vitae,
            mollis velit.
          </p>
        </div>
      </div>
    </Page>
  </div>
);

export const MasterDetail = Template.bind({});
MasterDetail.args = {};
