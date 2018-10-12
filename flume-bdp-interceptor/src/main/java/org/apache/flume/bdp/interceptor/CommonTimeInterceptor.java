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

public class CommonTimeInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory.getLogger(CommonTimeInterceptor.class);

  private final String key;
  private final boolean isTimeStamp;
  private final String sourcePattern;
  private final String targetPattern;

  /**
   * Only {@link CommonTimeInterceptor.Builder} can build me
   */
  private CommonTimeInterceptor(boolean isTimeStamp, String key,
                                String sourcePattern,String targetPattern) {
    this.isTimeStamp = isTimeStamp;
    this.key = key;
    this.sourcePattern = sourcePattern;
    this.targetPattern=targetPattern;
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
    Date dt=Calendar.getInstance().getTime();

    if (jsonObject.has(key)) {
      if(isTimeStamp){
          Long timeStr= jsonObject.get(key).getAsLong();
          dt=new Date(timeStr);
      }
      else{
        try {
          String timeStr= jsonObject.get(key).getAsString();
          dt=new SimpleDateFormat(sourcePattern).parse(timeStr);
        } catch (ParseException e) {
          logger.error(e.getMessage());
        }
      }
    }else{
      logger.error("jsonObject has no key: "+key);
    }
    headers.put("dt",new SimpleDateFormat(targetPattern).format(dt));
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
   * Builder which builds new instance of the org.apache.flume.bdp.interceptor.CommonTimeInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private String key;
    private boolean isTimeStamp;
    private String sourcePattern;
    private String targetPattern;

    @Override
    public void configure(Context context) {
      isTimeStamp = context.getBoolean("isTimeStamp", Constants.IS_TIMESTAMP_DEFAULT);
      key = context.getString("key", Constants.KEY_DEFAULT);
      sourcePattern = context.getString("sourcePattern", Constants.SOURCE_DEFAULT);
      targetPattern=context.getString("targetPattern", Constants.TARGET_DEFAULT);
    }

    @Override
    public Interceptor build() {
      logger.info(String.format(
          "Creating org.apache.flume.bdp.interceptor.CommonTimeInterceptor: isTimeStamp=%s,key=%s,sourcePattern=%s,targetPattern=%s",
              isTimeStamp, key, sourcePattern,targetPattern));
      return new CommonTimeInterceptor(isTimeStamp, key, sourcePattern,targetPattern);
    }

  }

  public static class Constants {
    public static final boolean IS_TIMESTAMP_DEFAULT = false;
    public static final String KEY_DEFAULT = "time";
    public static final String SOURCE_DEFAULT = "yyyy-MM-dd";
    public static final String TARGET_DEFAULT = "yyyyMMdd";
  }
}
