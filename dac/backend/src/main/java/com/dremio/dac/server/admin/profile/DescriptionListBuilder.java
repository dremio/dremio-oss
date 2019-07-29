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
package com.dremio.dac.server.admin.profile;

/**
 * Used to build a description list == list of description items, each of them having a "definition" and a "description"
 */
public class DescriptionListBuilder {

  private StringBuilder sb;
  public DescriptionListBuilder() {
    sb = new StringBuilder();
    sb.append("<dl class=\"dl-horizontal info-list\">");
  }

  public void addItem(final String termDefinition, final String termDescription) {
    sb.append("<dt>");
    sb.append(termDefinition);
    sb.append("</dt>\n<dd>");
    sb.append(termDescription);
    sb.append("</dd>\n");
  }

  public String build() {
    String rv;
    rv = sb.append("</dl>").toString();
    sb = null;
    return rv;
  }
}
