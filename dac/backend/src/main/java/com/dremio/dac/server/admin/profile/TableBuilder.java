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

class TableBuilder extends ProfileBuilder {
  private StringBuilder sb;
  private int w = 0;
  private int width;

  enum Alignment {
    CENTER,
    LEFT,
    RIGHT,
    JUSTIFY,
    DEFAULT
  }

  public TableBuilder(final String[] columns) {
    this(columns, Alignment.DEFAULT);
  }

  public TableBuilder(final String[] columns, Alignment alignment) {
    sb = new StringBuilder();
    width = columns.length;
    String textAlignment = getTextAlignment(alignment);
    String headerAlign = getHeaderAlignment(alignment);

    getFormat().setMaximumFractionDigits(3);

    sb.append("<table class=\"table ").append(textAlignment).append("\">\n<thead>\n<tr>");
    for (final String cn : columns) {
      sb.append(headerAlign).append(cn).append("</th>");
    }
    sb.append("</thead>\n</tr>\n");
  }

  @Override
  public ProfileBuilder appendCell(final String s) {
    if (w == 0) {
      sb.append("<tr>");
    }
    sb.append(String.format("<td>%s</td>", s));
    if (++w >= width) {
      sb.append("</tr>\n");
      w = 0;
    }
    return this;
  }

  public void appendRepeated(final String s, final int n) {
    for (int i = 0; i < n; i++) {
      appendCell(s);
    }
  }

  public String build() {
    String rv;
    rv = sb.append("\n</table>").toString();
    sb = null;
    return rv;
  }

  String getTextAlignment(Alignment alignment) {
    switch (alignment) {
      case CENTER:
        return "text-center";
      case LEFT:
        return "text-left";
      case JUSTIFY:
        return "text-justify";
      case RIGHT:
      case DEFAULT:
      default:
        return "text-right";
    }
  }

  String getHeaderAlignment(Alignment alignment) {
    switch (alignment) {
      case CENTER:
        return "<th style=\"text-align:center;\">";
      case LEFT:
        return "<th style=\"text-align:left;\">";
      case RIGHT:
        return "<th style=\"text-align:right;\">";
      case JUSTIFY:
        return "<th style=\"text-align:justify;\">";
      case DEFAULT:
      default:
        return "<th>";
    }
  }
}
