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

package io.crate.monitor;

import java.util.Collections;

public class ZeroExtendedNodeInfo implements ExtendedNodeInfo {

    private static final ExtendedNetworkStats NETWORK_STATS = new ExtendedNetworkStats(new ExtendedNetworkStats.Tcp());
    private static final ExtendedNetworkInfo NETWORK_INFO = new ExtendedNetworkInfo(ExtendedNetworkInfo.NA_INTERFACE);
    private static final ExtendedFsStats FS_STATS = new ExtendedFsStats(new ExtendedFsStats.Info());
    private static final ExtendedOsStats OS_STATS = new ExtendedOsStats(new ExtendedOsStats.Cpu());
    private static final ExtendedOsInfo OS_INFO = new ExtendedOsInfo(Collections.<String, Object>emptyMap());
    private static final ExtendedProcessCpuStats PROCESS_CPU_STATS = new ExtendedProcessCpuStats();

    @Override
    public ExtendedNetworkStats networkStats() {
        return NETWORK_STATS;
    }

    @Override
    public ExtendedNetworkInfo networkInfo() {
        return NETWORK_INFO;
    }

    @Override
    public ExtendedFsStats fsStats() {
        return FS_STATS;
    }

    @Override
    public ExtendedOsStats osStats() {
        return OS_STATS;
    }

    @Override
    public ExtendedOsInfo osInfo() {
        return OS_INFO;
    }

    @Override
    public ExtendedProcessCpuStats processCpuStats() {
        return PROCESS_CPU_STATS;
    }
}
