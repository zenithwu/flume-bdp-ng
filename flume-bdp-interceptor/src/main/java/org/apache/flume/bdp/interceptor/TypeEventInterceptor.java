/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.bdp.interceptor;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TypeEventInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory.getLogger(TypeEventInterceptor.class);


  /**
   * Only {@link TypeEventInterceptor.Builder} can build me
   */
  private TypeEventInterceptor() {
  }

  @Override
  public void initialize() {
    // no-op
  }

  /**
   * Modifies events in-place.
   */
  @Override
  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();
    JsonObject jsonObject= new JsonParser().parse(new String(event.getBody(),Charset.forName("utf-8"))).getAsJsonObject();
    if (jsonObject.has("type")) {
      headers.put("log_type",jsonObject.get("type").getAsString());
    }else{
      headers.put("log_type","log_type");
    }
    if (jsonObject.has("event")) {
      headers.put("log_event",jsonObject.get("event").getAsString());
    }else{
      headers.put("log_event","log_event");
    }
    return event;
  }

  /**
   * Delegates to {@link #intercept(Event)} in a loop.
   * @param events
   * @return
   */
  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  @Override
  public void close() {
    // no-op
  }

  /**
   * Builder which builds new instance of the org.apache.flume.bdp.interceptor.TypeEventInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    @Override
    public void configure(Context context) {

    }

    @Override
    public Interceptor build() {
      logger.info(String.format(
          "Creating org.apache.flume.bdp.interceptor.TypeEventInterceptor"));
      return new TypeEventInterceptor();
    }

  }
}
