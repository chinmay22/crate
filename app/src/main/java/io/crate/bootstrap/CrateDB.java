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

package io.crate.bootstrap;

import org.elasticsearch.bootstrap.BootstrapProxy;
import org.elasticsearch.bootstrap.StartupErrorProxy;

/**
 * A main entry point when starting from the command line.
 */
public class CrateDB {

    /**
     * Required method that's called by Apache Commons procrun when
     * running as a service on Windows, when the service is stopped.
     * <p>
     * http://commons.apache.org/proper/commons-daemon/procrun.html
     * <p>
     * NOTE: If this method is renamed and/or moved, make sure to update crate.bat!
     */
    static void close(String[] args) {
        BootstrapProxy.stop();
    }

    public static void main(String[] args) {
        System.setProperty("es.foreground", "yes");
        String[] startArgs = new String[args.length + 1];
        startArgs[0] = "start";
        System.arraycopy(args, 0, startArgs, 1, args.length);
        try {
            BootstrapProxy.init(startArgs);
        } catch (Throwable t) {
            // format exceptions to the console in a special way
            // to avoid 2MB stacktraces from guice, etc.
            throw new StartupErrorProxy(t);
        }
    }
}
