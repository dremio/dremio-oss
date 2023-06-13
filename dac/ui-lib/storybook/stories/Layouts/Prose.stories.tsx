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
  title: "Layouts/Prose",
};

export const Default = () => {
  return (
    <div className="dremio-prose">
      <p>
        <code>.dremio-prose</code> is a layout class that automatically formats
        longer sentences and paragraphs of text. It sets a maximum width of{" "}
        <code>75ch</code> to ensure long blocks of text don’t become unbearably
        long. It also sets a common vertical gap between paragraphs, headings,
        and other elements to establish a predictable vertical rythm.
      </p>
      <p>
        It’s particularly useful within explanatory tooltips, cards, and dialogs
        to limit their maximum width. Limiting a container component’s width by
        limiting the width of its children, such as by using{" "}
        <code>.dremio-prose</code>, is a more responsive and sustainable way to
        lay out components.
      </p>
    </div>
  );
};

Default.storyName = "Prose";
