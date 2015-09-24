/*
 * Copyright 2014-2015
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package brooklyn.location.docker.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.location.docker.DockerLocation;

/**
 * Block until a host is available to provision on. This is suitable when scaling is being handled by an autoscaler policy.
 * @author Matt Champion on 22/09/15
 */
public final class WaitForHostStrategy implements NoAvailableHostStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(WaitForHostStrategy.class);
    private final long backOff;
    private final int maxAttempts;
    private int attempts = 0;

    /**
     * Constructor. Creates a strategy with no timeout.
     * @param backOff The amount of time to wait between attempts
     */
    public WaitForHostStrategy(long backOff) {
        this(backOff, -1);
    }

    /**
     * Constructor. Creates a strategy with no timeout.
     * @param backOff The amount of time to wait between attempts
     * @param maxAttempts The maximum number of attempts to provision a container
     */
    public WaitForHostStrategy(long backOff, int maxAttempts) {
        this.backOff = backOff;
        this.maxAttempts = maxAttempts;
    }

    @Override
    public boolean handleNoHosts(DockerLocation location) {
        LOG.trace("Back off and wait for host");
        try {
            Thread.sleep(backOff);
        }
        catch (InterruptedException e) {
            LOG.info("Was interrupted while waiting for machine", e);
            return false;
        }

        if (attempts == maxAttempts) {
            return false;
        }
        else if (maxAttempts != -1) {
            attempts++;
        }

        return true;
    }
}
