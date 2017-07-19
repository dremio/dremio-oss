/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.model.common;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.dremio.datastore.Converter;

import javassist.Modifier;

/**
 * Generic implementation of the visitor pattern for protostuff generated classes
 *
 * The base class will have the following:
 * <pre>
 * &#64;JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "type")
 * &#64;JsonTypeIdResolver(TransformTypeIdResolver.class)
 * &#64;TypesEnum(types = MyBaseEnum.class, format = "com.dremio.my.package.MyBase%s")
 * public abstract class MyBase {
 *   private static final Acceptor&lt;MyBase, MyBaseVisitor&lt;?>, Wrapper> acceptor = new Acceptor&lt;MyBase, MyBaseVisitor&lt;?>, Wrapper>(){};
 *
 *   public final &lt;T> T accept(MyBaseVisitor&lt;T> visitor) throws VisitorException {
 *     return acceptor.accept(visitor, this);
 *   }
 *
 *   public interface MyBaseVisitor&lt;T> {
 *     T visit(MyBaseFoo foo) throws Exception;
 *     ...
 *   }
 * }
 * </pre>
 * @param <C> the base class of the protos
 * @param <V> the Visitor type. Typically MyVisitor<?>
 * @param <W> the Wrapper type. Must have a type field and a field for each type
 */
public abstract class Acceptor<C, V, W> {

  private Map<Class<?>, Method> visitorMethod = new HashMap<>();

  private Map<Class<?>, Method> wrapperSetter = new HashMap<>();

  private Map<Enum<?>, Method> wrapperGetter = new HashMap<>();

  private Map<Class<?>, Enum<?>> visitedType = new HashMap<>();

  private Method getWrapperTypeMethod;

  private Method setWrapperTypeMethod;

  private Class<W> wrapper;

  private Class<C> baseType;

  public Acceptor() {
    Type type = getClass().getGenericSuperclass();
    Type[] actualTypeArguments = ((ParameterizedType)type).getActualTypeArguments();

    @SuppressWarnings("unchecked") // by definition the 1st argument is a Class<C>
    Class<C> baseClass = (Class<C>)actualTypeArguments[0];
    this.baseType = baseClass;

    ParameterizedType visitorType = ((ParameterizedType)actualTypeArguments[1]);

    @SuppressWarnings("unchecked") // by definition the 3rd argument is a Class<W>
    Class<W> w = (Class<W>)actualTypeArguments[2];
    this.wrapper = w;

    indexVisitorMethods(visitorType);

    // Validate Visitor completeness
    Map<String, Class<?>> foundClasses = new HashMap<>();
    Set<Class<?>> classesMissingBase = new HashSet<>();
    for (Class<?> c : visitorMethod.keySet()) {
      foundClasses.put(c.getName(), c);
      if (!baseClass.isAssignableFrom(c)) {
        classesMissingBase.add(c);
      }
    }

    if (!classesMissingBase.isEmpty()) {
      throw new IllegalArgumentException(String.format(
          "some classes do not extends the base type %s: %s",
          baseClass, classesMissingBase));
    }

    TypesEnum typesEnum = baseClass.getAnnotation(TypesEnum.class);

    SubTypeMapping mapping = new SubTypeMapping(typesEnum);
    for (Enum<?> e : mapping.getEnumConstants()) {
      String className = mapping.getClassName(e);
      Class<?> c = foundClasses.remove(className);
      if (c == null) {
        throw new IllegalArgumentException(String.format(
            "Class %s not found in visitor %s for enum value %s",
            className, visitorType.getRawType(), e));
      } else {
        visitedType.put(c, e);
      }
    }
    if (!foundClasses.isEmpty()) {
      throw new IllegalArgumentException(String.format("Missing enums for classes %s", foundClasses));
    }

    indexWrapperGettersAndSetters(typesEnum.types());
    if (wrapperGetter.size() < visitorMethod.size()) {
      throw new IllegalArgumentException(String.format("Missing getters in wrapper %s", wrapper));
    }
    if (wrapperSetter.size() < visitorMethod.size()) {
      throw new IllegalArgumentException(String.format("Missing setters in wrapper %s", wrapper));
    }
  }

  private void indexVisitorMethods(ParameterizedType visitorType) {
    Class<?> vc = (Class<?>)visitorType.getRawType();
    for (Method method : vc.getMethods()) {
      if (method.getName().equals("visit") && method.getParameterTypes().length == 1
          && Modifier.isAbstract(method.getModifiers())) {
        Class<?> visited = method.getParameterTypes()[0];
        visitorMethod.put(visited, method);
      }
    }
  }

  private void indexWrapperGettersAndSetters(Class<?> typesEnum) {
    for (Method method : wrapper.getMethods()) {
      if (method.getName().startsWith("set") && method.getParameterTypes().length == 1) {
        Class<?> setClass = method.getParameterTypes()[0];
        if (setClass.equals(typesEnum)) {
          if (setWrapperTypeMethod != null) {
            throw new IllegalArgumentException(String.format(
                "Two type setters: %s and %s",
                method, getWrapperTypeMethod));
          }
          setWrapperTypeMethod = method;
        } else if (visitorMethod.containsKey(setClass)) {
          Method previous = wrapperSetter.put(setClass, method);
          if (previous != null) {
            throw new IllegalArgumentException(String.format(
                "Two setters for %s: %s and %s",
                setClass, method, previous));
          }
        }
      } else if (method.getName().startsWith("get") && method.getParameterTypes().length == 0) {
        Class<?> getClass = method.getReturnType();
        if (getClass.equals(typesEnum)) {
          if (getWrapperTypeMethod != null) {
            throw new IllegalArgumentException(String.format(
                "Two type getters: %s and %s",
                method, getWrapperTypeMethod));
          }
          getWrapperTypeMethod = method;
        } else if (visitorMethod.containsKey(getClass)) {
          Method previous = wrapperGetter.put(visitedType.get(getClass), method);
          if (previous != null) {
            throw new IllegalArgumentException(String.format(
                "Two getters for %s: %s and %s",
                getClass, method, previous));
          }
        }
      }
    }
  }

  public final <T> T accept(V visitor, C visited) throws VisitorException {
    Method method = findMethod(visited);
    try {
      @SuppressWarnings("unchecked")
      T result = (T) method.invoke(visitor, visited);
      return result;
    } catch (IllegalAccessException e) {
      throw new VisitorException("method not accessible", e);
    } catch (InvocationTargetException e) {
      Throwable targetException = e.getTargetException();
      if (targetException instanceof RuntimeException) {
        throw (RuntimeException)targetException;
      } else if (targetException instanceof Error) {
        throw (Error)targetException;
      } else if (targetException instanceof Exception) {
        throw new VisitorException("visitor threw an exception visiting " + visited, (Exception)targetException);
      } else {
        // can not happen
        throw new RuntimeException("visitor threw an exception visiting " + visited, targetException);
      }
    }
  }

  private Method findMethod(C visited) {
    return find(visited, visitorMethod);
  }

  private <T> T find(C c, Map<Class<?>, T> map) {
    Class<? extends Object> visitedClass = c.getClass();
    T searched = map.get(visitedClass);
    while (visitedClass != null && searched == null) {
      visitedClass = visitedClass.getSuperclass();
      searched = map.get(visitedClass);
    }
    if (searched == null) {
      throw new IllegalArgumentException("not found " + c + " in " + map.keySet());
    } else if (visitedClass != c) {
      // add it if we had to lookup through parent
      map.put(c.getClass(), searched);
    }
    return searched;
  }

  public Enum<?> getType(C subInstance) {
    return find(subInstance, visitedType);
  }

  public W wrap(C subInstance) {
    W newInstance;
    try {
      newInstance = wrapper.newInstance();
      wrap(subInstance, newInstance);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException e) {
      throw new RuntimeException("can't wrap " + subInstance, e);
    }
    return newInstance;
  }

  private void wrap(C subInstance, W newInstance) {
    try {
      setWrapperTypeMethod.invoke(newInstance, getType(subInstance));
      wrapperSetter.get(subInstance.getClass()).invoke(newInstance, subInstance);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException("can't wrap " + subInstance, e);
    }
  }

  public C unwrap(W wrapper) {
    if (wrapper == null) {
      return null;
    }
    try {
      Enum<?> type = (Enum<?>)getWrapperTypeMethod.invoke(wrapper);
      return baseType.cast(wrapperGetter.get(type).invoke(wrapper));
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException("can't unwrap " + wrapper, e);
    }
  }

  public Class<C> getBaseType() {
    return baseType;
  }

  public Class<W> getWrapperType() {
    return wrapper;
  }

  public Converter<C, W> converter() {
    return new Converter<C, W>() {
      @Override
      public W convert(C v) {
        return wrap(v);
      }
      @Override
      public C revert(W v) {
        return unwrap(v);
      }
    };
  }

}
