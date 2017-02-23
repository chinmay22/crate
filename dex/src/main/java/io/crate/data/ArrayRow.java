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

package io.crate.data;

import java.util.Arrays;

/**
 * A Row that is similar to {@link RowN} in that is uses a backing array which can be changed.
 * <p>
 * The only difference is that size is always derived from the backing array and not changeable.
 */
public class ArrayRow implements Row {

    private Object[] cells;

    @Override
    public int numColumns() {
        return cells.length;
    }

    @Override
    public Object get(int index) {
        return cells[index];
    }

    @Override
    public Object[] materialize() {
        Object[] copy = new Object[cells.length];
        System.arraycopy(cells, 0, copy, 0, cells.length);
        return copy;
    }

    public void cells(Object[] cells) {
        this.cells = cells;
    }

    @Override
    public String toString() {
        return "ArrayRow{" +
               "cells=" + Arrays.toString(cells) +
               '}';
    }
}
