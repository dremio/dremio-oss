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
  title: "Utilities/Rounded",
};

const roundedScale = ["none", "sm", "md", "lg", "xl", "2xl", "3xl"];

export const Default = () => {
  return (
    <div>
      {roundedScale.map((rounded) => (
        <div
          key={rounded}
          className={`bg-brand-300 items-center m-4 p-5 rounded-${rounded} w-12 h-10`}
          style={{ display: "inline-flex" }}
        >
          <code>.rounded-{rounded}</code>
        </div>
      ))}
    </div>
  );
};

Default.storyName = "Rounded";
