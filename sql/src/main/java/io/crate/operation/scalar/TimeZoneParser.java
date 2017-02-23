/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.operation.scalar;

import io.crate.analyze.symbol.Literal;
import org.apache.lucene.util.BytesRef;
import org.joda.time.DateTimeZone;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * parses timezone strings to {@linkplain DateTimeZone} instances
 */
public class TimeZoneParser {

    public static final DateTimeZone DEFAULT_TZ = DateTimeZone.UTC;
    public static final Literal<BytesRef> DEFAULT_TZ_LITERAL = Literal.of("UTC");
    public static final BytesRef DEFAULT_TZ_BYTES_REF = DEFAULT_TZ_LITERAL.value();

    private static final ConcurrentMap<BytesRef, DateTimeZone> TIME_ZONE_MAP = new ConcurrentHashMap<>();

    private TimeZoneParser() {
    }

    public static DateTimeZone parseTimeZone(BytesRef timezone) throws IllegalArgumentException {
        if (timezone == null) {
            throw new IllegalArgumentException("invalid time zone value NULL");
        }
        if (timezone.equals(DEFAULT_TZ_BYTES_REF)) {
            return DEFAULT_TZ;
        }

        DateTimeZone tz = TIME_ZONE_MAP.get(timezone);
        if (tz == null) {
            try {
                String text = timezone.utf8ToString();
                int index = text.indexOf(':');
                if (index != -1) {
                    int beginIndex = text.charAt(0) == '+' ? 1 : 0;
                    // format like -02:30
                    tz = DateTimeZone.forOffsetHoursMinutes(
                        Integer.parseInt(text.substring(beginIndex, index)),
                        Integer.parseInt(text.substring(index + 1))
                    );
                } else {
                    // id, listed here: http://joda-time.sourceforge.net/timezones.html
                    // or here: http://www.joda.org/joda-time/timezones.html
                    tz = DateTimeZone.forID(text);
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "invalid time zone value '%s'", timezone.utf8ToString()));
            }
            TIME_ZONE_MAP.putIfAbsent(timezone, tz);
        }
        return tz;
    }
}
