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

import { Meta, StoryFn } from "@storybook/react";

import { IconButton, Popover } from "../../../components";

import createViewIcon from "../../../images/walkthrough/create-view.svg";
import addUsersIcon from "../../../images/walkthrough/add-users.svg";
import addDatasourceIcon from "../../../images/walkthrough/add-datasource.svg";

export default {
  title: "Components/Popover",
  component: Popover,
} as Meta<typeof Popover>;

export const Default: StoryFn<typeof Popover> = () => {
  return (
    <Popover
      mode="hover"
      placement="right"
      content={
        <div className="leading-normal drop-shadow-lg">
          <header className="text-lg text-semibold bg-brand-500 py-1 px-2 flex flex-row items-center gap-1 rounded-top-sm">
            <dremio-icon
              name="interface/tutorial"
              alt=""
              style={{ width: "2em", height: "2em" }}
            ></dremio-icon>{" "}
            Get started with Sonar catalog
            <span className="ml-auto">
              <IconButton
                tooltip="Close"
                style={{ width: "1em", height: "1em" }}
              >
                <dremio-icon name="interface/close-big"></dremio-icon>
              </IconButton>
            </span>
          </header>
          <div
            className="bg-foreground p-1 rounded-bottom-sm"
            style={{ maxWidth: "40ch" }}
          >
            <p className="text-semibold color-faded">
              Sonar is a blaha blah blah... To get started, follow the steps
              under the get started guide.
            </p>
            <ul className="mt-2 flex flex-col gap-2">
              <li className="flex flex-row gap-105">
                <img src={createViewIcon} className="h-7 w-7" />
                <div className="flex flex-col">
                  <span className="text-semibold">Create a view</span>
                  <span className="color-faded text-sm">
                    Best way to experience querying data with sub-second
                    performance.
                  </span>
                </div>
              </li>
              <li className="flex flex-row gap-105">
                <img src={createViewIcon} className="h-7 w-7" />
                <div className="flex flex-col">
                  <span className="text-semibold">Build a data product</span>
                  <span className="color-faded text-sm">
                    Build a data view, compose a wiki and explore lineage graph
                  </span>
                </div>
              </li>
              <li className="flex flex-row gap-105">
                <img src={addDatasourceIcon} className="h-7 w-7" />
                <div className="flex flex-col">
                  <span className="text-semibold">Add a data source</span>
                  <span className="color-faded text-sm">
                    Add your own data source to experience...
                  </span>
                </div>
              </li>
              <li className="flex flex-row gap-105">
                <img src={addUsersIcon} className="h-7 w-7" />
                <div className="flex flex-col">
                  <span className="text-semibold">
                    Add users to organization{" "}
                  </span>
                  <span className="color-faded text-sm">
                    Go to documentation or web page?!
                  </span>
                </div>
              </li>
            </ul>
          </div>
        </div>
      }
    >
      <div style={{ display: "inline-flex" }}>Hover me</div>
    </Popover>
  );
};

Default.storyName = "Popover";
