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

export default {
  title: "Components/Listbox",
};

export const Default = () => (
  <ul className="float-container listbox float-container-enter-done max-w-max">
    <li className="listbox-group-label">AWS</li>
    <ul className="listbox-group">
      <li className=" listbox-item">
        <span>
          US West (N. California) <small>us-west-1</small>
        </span>
      </li>
      <li className=" listbox-item">
        <span>
          US West (Oregon) <small>us-west-2</small>
        </span>
      </li>
      <li className=" listbox-item">
        <span>
          US East (N. Virginia) <small>us-east-1</small>
        </span>
      </li>
      <li className=" listbox-item">
        <span>
          US East (Ohio) <small>us-east-2</small>
        </span>
      </li>
      <li className=" listbox-item">
        <span>
          Canada (Central) <small>ca-central-1</small>
        </span>
      </li>
    </ul>
    <li className="listbox-group-label">Azure</li>
    <ul className="listbox-group">
      <li className=" listbox-item">
        <span>
          East US <small>east-us</small>
        </span>
      </li>
      <li className=" listbox-item">
        <span>
          East US 2 <small>east-us-2</small>
        </span>
      </li>
    </ul>
  </ul>
);

Default.storyName = "Listbox";
