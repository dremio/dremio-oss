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
package com.dremio.dac.server;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.CharBuffer;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ReadListener;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verbose Access log filter to help debugging
 */
final class AccessLogFilter implements Filter {
  private static final Logger logger = LoggerFactory.getLogger(AccessLogFilter.class);

  private long nextReqId = 0;

  private PrintWriter docLog;

  private Set<String> printableContentTypes = new HashSet<>(asList(
      "text/html; charset=UTF-8",
      "application/json",
      "text/plain"));

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  private void log(String message) {
    logger.debug(message);
    if (docLog != null) {
      docLog.println(message);
    }
  }

  String bodyToString(String contentType, StringWriter body) {
    return
        printableContentTypes.contains(contentType) ?
            body.toString() :
              contentType + " length: " + body.getBuffer().length();
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    long reqId = nextReqId ++;
    HttpServletRequest req = (HttpServletRequest)request;
    HttpServletResponse resp = (HttpServletResponse)response;

    StringBuffer requestURL = req.getRequestURL();
    String queryString = req.getQueryString();
    final String fullUrl;
    if (queryString == null) {
        fullUrl = requestURL.toString();
    } else {
      fullUrl = requestURL.append('?').append(queryString).toString();
    }


    log(format("%d: %s %s", reqId, req.getMethod(), fullUrl));
    String referer = req.getHeader("Referer");
    if (referer != null) {
      log(format("%d: referer=%s", reqId, referer));
    }
    StringWriter reqBody = new StringWriter();
    StringWriter respBody = new StringWriter();
    long t0 = System.currentTimeMillis();
    chain.doFilter(new HTTPRequestWrapper(req, reqBody), new HTTPResponseWrapper(resp, respBody));
    long t1 = System.currentTimeMillis();
    if (reqBody.toString().trim().length() > 0) {
      log(reqId + ": request body:\n" + prefixLines(reqId + ": => ", bodyToString(req.getContentType(), reqBody)));
    }
    log(String.format("%d: Status %s, Content-type: %s, returned in %dms", reqId, resp.getStatus(), resp.getContentType(), t1 - t0));
    if (respBody.getBuffer().length() > 0) {
      log(reqId + ": response body:\n" + prefixLines(reqId + ": <= ", bodyToString(resp.getContentType(), respBody)));

    }
  }

  private String prefixLines(String prefix, String string) {
    String[] lines = string.toString().split("\n");
    StringBuilder sb = new StringBuilder();
    for (String line: lines) {
      sb.append(prefix).append(line).append("\n");
    }
    return sb.toString();
  }

  @Override
  public void destroy() {
  }

  private static class HTTPResponseWrapper implements HttpServletResponse {
    private final HttpServletResponse d;
    private StringWriter respBody;

    public HTTPResponseWrapper(HttpServletResponse d, StringWriter respBody) {
      super();
      this.d = d;
      this.respBody = respBody;
    }

    @Override
    public void addCookie(Cookie cookie) {
      d.addCookie(cookie);
    }

    @Override
    public boolean containsHeader(String name) {
      return d.containsHeader(name);
    }

    @Override
    public String encodeURL(String url) {
      return d.encodeURL(url);
    }

    @Override
    public String getCharacterEncoding() {
      return d.getCharacterEncoding();
    }

    @Override
    public String encodeRedirectURL(String url) {
      return d.encodeRedirectURL(url);
    }

    @Override
    public String getContentType() {
      return d.getContentType();
    }

    @Override
    @Deprecated
    public String encodeUrl(String url) {
      return d.encodeUrl(url);
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {

      final ServletOutputStream outputStream = d.getOutputStream();
      return new ServletOutputStream() {

        @Override
        public void write(int b) throws IOException {
          respBody.write(b);
          outputStream.write(b);
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
          outputStream.setWriteListener(writeListener);
        }

        @Override
        public boolean isReady() {
          return outputStream.isReady();
        }
      };
    }

    @Override
    @Deprecated
    public String encodeRedirectUrl(String url) {
      return d.encodeRedirectUrl(url);
    }

    @Override
    public void sendError(int sc, String msg) throws IOException {
      d.sendError(sc, msg);
    }

    @Override
    public PrintWriter getWriter() throws IOException {
      return new PrintWriter(new WrappedWriter(d.getWriter(), respBody));
    }

    @Override
    public void sendError(int sc) throws IOException {
      d.sendError(sc);
    }

    @Override
    public void setCharacterEncoding(String charset) {
      d.setCharacterEncoding(charset);
    }

    @Override
    public void sendRedirect(String location) throws IOException {
      d.sendRedirect(location);
    }

    @Override
    public void setContentLength(int len) {
      d.setContentLength(len);
    }

    @Override
    public void setContentLengthLong(long len) {
      d.setContentLengthLong(len);
    }

    @Override
    public void setDateHeader(String name, long date) {
      d.setDateHeader(name, date);
    }

    @Override
    public void setContentType(String type) {
      d.setContentType(type);
    }

    @Override
    public void addDateHeader(String name, long date) {
      d.addDateHeader(name, date);
    }

    @Override
    public void setHeader(String name, String value) {
      d.setHeader(name, value);
    }

    @Override
    public void setBufferSize(int size) {
      d.setBufferSize(size);
    }

    @Override
    public void addHeader(String name, String value) {
      d.addHeader(name, value);
    }

    @Override
    public void setIntHeader(String name, int value) {
      d.setIntHeader(name, value);
    }

    @Override
    public int getBufferSize() {
      return d.getBufferSize();
    }

    @Override
    public void addIntHeader(String name, int value) {
      d.addIntHeader(name, value);
    }

    @Override
    public void flushBuffer() throws IOException {
      d.flushBuffer();
    }

    @Override
    public void setStatus(int sc) {
      d.setStatus(sc);
    }

    @Override
    public void resetBuffer() {
      d.resetBuffer();
    }

    @Override
    public boolean isCommitted() {
      return d.isCommitted();
    }

    @Override
    @Deprecated
    public void setStatus(int sc, String sm) {
      d.setStatus(sc, sm);
    }

    @Override
    public void reset() {
      d.reset();
    }

    @Override
    public int getStatus() {
      return d.getStatus();
    }

    @Override
    public String getHeader(String name) {
      return d.getHeader(name);
    }

    @Override
    public void setLocale(Locale loc) {
      d.setLocale(loc);
    }

    @Override
    public Collection<String> getHeaders(String name) {
      return d.getHeaders(name);
    }

    @Override
    public Collection<String> getHeaderNames() {
      return d.getHeaderNames();
    }

    @Override
    public Locale getLocale() {
      return d.getLocale();
    }

  }


  private static class HTTPRequestWrapper implements HttpServletRequest {
    private final HttpServletRequest d;
    private StringWriter reqBody;

    public HTTPRequestWrapper(HttpServletRequest d, StringWriter reqBody) {
      super();
      this.d = d;
      this.reqBody = reqBody;
    }

    @Override
    public Object getAttribute(String name) {
      return d.getAttribute(name);
    }

    @Override
    public String getAuthType() {
      return d.getAuthType();
    }

    @Override
    public Cookie[] getCookies() {
      return d.getCookies();
    }

    @Override
    public Enumeration<String> getAttributeNames() {
      return d.getAttributeNames();
    }

    @Override
    public long getDateHeader(String name) {
      return d.getDateHeader(name);
    }

    @Override
    public String getCharacterEncoding() {
      return d.getCharacterEncoding();
    }

    @Override
    public void setCharacterEncoding(String env) throws UnsupportedEncodingException {
      d.setCharacterEncoding(env);
    }

    @Override
    public int getContentLength() {
      return d.getContentLength();
    }

    @Override
    public String getHeader(String name) {
      return d.getHeader(name);
    }

    @Override
    public long getContentLengthLong() {
      return d.getContentLengthLong();
    }

    @Override
    public Enumeration<String> getHeaders(String name) {
      return d.getHeaders(name);
    }

    @Override
    public String getContentType() {
      return d.getContentType();
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
      final ServletInputStream inputStream = d.getInputStream();
      return new ServletInputStream() {

        @Override
        public int read() throws IOException {
          int b = inputStream.read();
          if (b != -1) {
            reqBody.write(b);
          }
          return b;
        }

        @Override
        public void setReadListener(ReadListener readListener) {
          inputStream.setReadListener(readListener);
        }

        @Override
        public boolean isReady() {
          return inputStream.isReady();
        }

        @Override
        public boolean isFinished() {
          return inputStream.isFinished();
        }
      };
    }

    @Override
    public String getParameter(String name) {
      return d.getParameter(name);
    }

    @Override
    public Enumeration<String> getHeaderNames() {
      return d.getHeaderNames();
    }

    @Override
    public int getIntHeader(String name) {
      return d.getIntHeader(name);
    }

    @Override
    public Enumeration<String> getParameterNames() {
      return d.getParameterNames();
    }

    @Override
    public String getMethod() {
      return d.getMethod();
    }

    @Override
    public String[] getParameterValues(String name) {
      return d.getParameterValues(name);
    }

    @Override
    public String getPathInfo() {
      return d.getPathInfo();
    }

    @Override
    public Map<String, String[]> getParameterMap() {
      return d.getParameterMap();
    }

    @Override
    public String getPathTranslated() {
      return d.getPathTranslated();
    }

    @Override
    public String getProtocol() {
      return d.getProtocol();
    }

    @Override
    public String getScheme() {
      return d.getScheme();
    }

    @Override
    public String getContextPath() {
      return d.getContextPath();
    }

    @Override
    public String getServerName() {
      return d.getServerName();
    }

    @Override
    public int getServerPort() {
      return d.getServerPort();
    }

    @Override
    public BufferedReader getReader() throws IOException {
      return new BufferedReader(new WrappedReader(d.getReader(), reqBody));
    }

    @Override
    public String getQueryString() {
      return d.getQueryString();
    }

    @Override
    public String getRemoteUser() {
      return d.getRemoteUser();
    }

    @Override
    public String getRemoteAddr() {
      return d.getRemoteAddr();
    }

    @Override
    public String getRemoteHost() {
      return d.getRemoteHost();
    }

    @Override
    public boolean isUserInRole(String role) {
      return d.isUserInRole(role);
    }

    @Override
    public void setAttribute(String name, Object o) {
      d.setAttribute(name, o);
    }

    @Override
    public Principal getUserPrincipal() {
      return d.getUserPrincipal();
    }

    @Override
    public void removeAttribute(String name) {
      d.removeAttribute(name);
    }

    @Override
    public String getRequestedSessionId() {
      return d.getRequestedSessionId();
    }

    @Override
    public Locale getLocale() {
      return d.getLocale();
    }

    @Override
    public String getRequestURI() {
      return d.getRequestURI();
    }

    @Override
    public Enumeration<Locale> getLocales() {
      return d.getLocales();
    }

    @Override
    public boolean isSecure() {
      return d.isSecure();
    }

    @Override
    public StringBuffer getRequestURL() {
      return d.getRequestURL();
    }

    @Override
    public RequestDispatcher getRequestDispatcher(String path) {
      return d.getRequestDispatcher(path);
    }

    @Override
    public String getServletPath() {
      return d.getServletPath();
    }

    @Override
    @Deprecated
    public String getRealPath(String path) {
      return d.getRealPath(path);
    }

    @Override
    public HttpSession getSession(boolean create) {
      return d.getSession(create);
    }

    @Override
    public int getRemotePort() {
      return d.getRemotePort();
    }

    @Override
    public String getLocalName() {
      return d.getLocalName();
    }

    @Override
    public String getLocalAddr() {
      return d.getLocalAddr();
    }

    @Override
    public int getLocalPort() {
      return d.getLocalPort();
    }

    @Override
    public ServletContext getServletContext() {
      return d.getServletContext();
    }

    @Override
    public HttpSession getSession() {
      return d.getSession();
    }

    @Override
    public AsyncContext startAsync() throws IllegalStateException {
      return d.startAsync();
    }

    @Override
    public String changeSessionId() {
      return d.changeSessionId();
    }

    @Override
    public boolean isRequestedSessionIdValid() {
      return d.isRequestedSessionIdValid();
    }

    @Override
    public boolean isRequestedSessionIdFromCookie() {
      return d.isRequestedSessionIdFromCookie();
    }

    @Override
    public boolean isRequestedSessionIdFromURL() {
      return d.isRequestedSessionIdFromURL();
    }

    @Override
    @Deprecated
    public boolean isRequestedSessionIdFromUrl() {
      return d.isRequestedSessionIdFromUrl();
    }

    @Override
    public boolean authenticate(HttpServletResponse response) throws IOException, ServletException {
      return d.authenticate(response);
    }

    @Override
    public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse)
        throws IllegalStateException {
      return d.startAsync(servletRequest, servletResponse);
    }

    @Override
    public void login(String username, String password) throws ServletException {
      d.login(username, password);
    }

    @Override
    public void logout() throws ServletException {
      d.logout();
    }

    @Override
    public Collection<Part> getParts() throws IOException, ServletException {
      return d.getParts();
    }

    @Override
    public boolean isAsyncStarted() {
      return d.isAsyncStarted();
    }

    @Override
    public boolean isAsyncSupported() {
      return d.isAsyncSupported();
    }

    @Override
    public Part getPart(String name) throws IOException, ServletException {
      return d.getPart(name);
    }

    @Override
    public AsyncContext getAsyncContext() {
      return d.getAsyncContext();
    }

    @Override
    public DispatcherType getDispatcherType() {
      return d.getDispatcherType();
    }

    @Override
    public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass) throws IOException, ServletException {
      return d.upgrade(handlerClass);
    }

  }

  public static class WrappedReader extends Reader {

    private Reader delegate;
    private StringWriter reqBody;

    public WrappedReader(Reader delegate, StringWriter reqBody) {
      this.delegate = delegate;
      this.reqBody = reqBody;
    }

    @Override
    public int read(CharBuffer target) throws IOException {
      int len = target.remaining();
      char[] cbuf = new char[len];
      int n = read(cbuf, 0, len);
      if (n > 0) {
          target.put(cbuf, 0, n);
      }
      return n;
    }

    @Override
    public int read(char[] cbuf) throws IOException {
      return read(cbuf, 0, cbuf.length);
    }

    @Override
    public int read() throws IOException {
      int read = delegate.read();
      reqBody.write(read);
      return read;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      int n = delegate.read(cbuf, off, len);
      for (int i = 0; i < n; i++) {
        char c = cbuf[off + i];
        reqBody.write(c);
      }
      return n;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

  }

  public static class WrappedWriter extends Writer {

    private PrintWriter d;
    private StringWriter respBody;

    public WrappedWriter(PrintWriter d, StringWriter respBody) {
      this.d = d;
      this.respBody = respBody;
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
      respBody.write(cbuf, off, len);
      d.write(cbuf, off, len);
    }

    @Override
    public void flush() throws IOException {
      d.flush();
    }

    @Override
    public void close() throws IOException {
      d.close();
    }

  }

  public void stopLoggingToFile() {
    if (docLog != null) {
      this.docLog.flush();
      this.docLog = null;
    }
  }

  public void startLoggingToFile(PrintWriter docLog) {
    this.docLog = docLog;
  }
}
