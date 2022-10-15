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
package com.dremio.common.expression;

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.expression.PathSegment.ArraySegment;
import com.dremio.common.expression.PathSegment.NameSegment;
import com.dremio.exec.proto.UserBitShared.NamePart;
import com.dremio.exec.proto.UserBitShared.NamePart.Type;

/**
 * A Basic path object that can be used with vectors and containers.
 */
public abstract class BasePath implements ProvidesUnescapedPath {

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final NameSegment rootSegment;

  public static BasePath getSimple(String name) {
    return new BasePath(new NameSegment(name)) {};
  }

  public PathSegment getLastSegment() {
    PathSegment s= rootSegment;
    while (s.getChild() != null) {
      s = s.getChild();
    }
    return s;
  }

  public NamePart getAsNamePart() {
    return getNamePart(rootSegment);
  }

  public <IN, OUT> OUT accept(SchemaPathVisitor<IN, OUT> visitor, IN in){
    return getRootSegment().accept(visitor, in);
  }

  public interface SchemaPathVisitor<IN, OUT> {
    public OUT visitName(NameSegment segment, IN in);
    public OUT visitArray(ArraySegment segment, IN in);
  }

  private static NamePart getNamePart(PathSegment s) {
    if (s == null) {
      return null;
    }
    NamePart.Builder b = NamePart.newBuilder();
    if (s.getChild() != null) {
      b.setChild(getNamePart(s.getChild()));
    }

    if (s.isArray()) {
      if (s.getArraySegment().hasIndex()) {
        throw new IllegalStateException("You cannot convert a indexed schema path to a NamePart.  NameParts can only reference Vectors, not individual records or values.");
      }
      b.setType(Type.ARRAY);
    } else {
      b.setType(Type.NAME);
      b.setName(s.getNameSegment().getPath());
    }
    return b.build();
  }

  public List<String> getNameSegments(){
    List<String> segments = new ArrayList<>();
    PathSegment seg = rootSegment;
    while(seg != null){
      if(seg.isNamed()){
        segments.add(seg.getNameSegment().getPath());
      }
      seg = seg.getChild();
    }
    return segments;
  }

  public List<String> getComplexNameSegments(){
    List<String> segments = new ArrayList<>();
    PathSegment seg = rootSegment;
    while(seg != null){
      if(seg.isNamed()){
        segments.add(seg.getNameSegment().getPath());
      } else if (seg.isArray()) {
        segments.add("list");
        segments.add("element");
      }
      seg = seg.getChild();
    }
    return segments;
  }

  protected static PathSegment getPathSegment(NamePart n) {
    PathSegment child = n.hasChild() ? getPathSegment(n.getChild()) : null;
    if (n.getType() == Type.ARRAY) {
      return new ArraySegment(child);
    } else {
      return new NameSegment(n.getName(), child);
    }
  }

  /**
   * A simple is a path where there are no repeated elements outside the lowest level of the path.
   * @return Whether this path is a simple path.
   */
  public boolean isSimplePath() {
    PathSegment seg = rootSegment;
    while (seg != null) {
      if (seg.isArray() && !seg.isLastPath()) {
        return false;
      }
      seg = seg.getChild();
    }
    return true;
  }

  protected BasePath(BasePath path) {
    this.rootSegment = path.rootSegment;
  }

  public BasePath(NameSegment rootSegment) {
    this.rootSegment = rootSegment;
  }

  public NameSegment getRootSegment() {
    return rootSegment;
  }

  @Override
  public String getAsUnescapedPath() {
    StringBuilder sb = new StringBuilder();
    PathSegment seg = getRootSegment();
    if (seg.isArray()) {
      throw new IllegalStateException("Dremio doesn't currently support top level arrays");
    }
    sb.append(seg.getNameSegment().getPath());

    while ( (seg = seg.getChild()) != null) {
      if (seg.isNamed()) {
        sb.append('.');
        sb.append(seg.getNameSegment().getPath());
      } else {
        sb.append('[');
        sb.append(seg.getArraySegment().getIndex());
        sb.append(']');
      }
    }
    return sb.toString();
  }

}
