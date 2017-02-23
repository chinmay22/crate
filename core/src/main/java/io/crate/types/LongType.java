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

package io.crate.types;

import io.crate.Streamer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class
LongType extends DataType<Long> implements FixedWidthType, Streamer<Long>, DataTypeFactory {

    public static final LongType INSTANCE = new LongType();
    public static final int ID = 10;

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "long";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    public Long value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof String) {
            return Long.valueOf((String) value);
        }
        if (value instanceof BytesRef) {
            return parseLong((BytesRef) value);
        }
        return ((Number) value).longValue();
    }

    /**
     * parses the utf-8 encoded bytesRef argument as signed decimal {@code long}.
     * All characters in the string must be decimal digits, except the first which may be an ASCII minus sign to indicate
     * a negative value or or a plus sign to indicate a positive value.
     * <p>
     * mostly copied from {@link Long#parseLong(String s, int radix)}
     */
    private long parseLong(BytesRef value) {
        assert value != null : "value must not be null";
        boolean negative = false;
        long result = 0;

        int i = value.offset;
        int len = value.length;
        int radix = 10;
        long limit = -Long.MAX_VALUE;
        long multmin;
        byte[] bytes = value.bytes;
        int digit;

        if (len <= 0) {
            throw new NumberFormatException(value.utf8ToString());
        }

        char firstChar = (char) bytes[i];
        if (firstChar < '0') {
            if (firstChar == '-') {
                negative = true;
                limit = Long.MIN_VALUE;
            } else if (firstChar != '+') {
                throw new NumberFormatException(value.utf8ToString());
            }

            if (len == 1) { // lone '+' or '-'
                throw new NumberFormatException(value.utf8ToString());
            }
            i++;
        }
        multmin = limit / radix;
        while (i < len + value.offset) {
            digit = Character.digit((char) bytes[i], radix);
            i++;
            if (digit < 0) {
                throw new NumberFormatException(value.utf8ToString());
            }
            if (result < multmin) {
                throw new NumberFormatException(value.utf8ToString());
            }
            result *= radix;
            if (result < limit + digit) {
                throw new NumberFormatException(value.utf8ToString());
            }
            result -= digit;
        }
        return negative ? result : -result;
    }

    @Override
    public int compareValueTo(Long val1, Long val2) {
        return nullSafeCompareValueTo(val1, val2, Long::compare);
    }

    @Override
    public Long readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readLong();
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeLong(((Number) v).longValue());
        }
    }

    @Override
    public DataType<?> create() {
        return INSTANCE;
    }

    @Override
    public int fixedSize() {
        return 16; // 8 object overhead, 8 long
    }
}

