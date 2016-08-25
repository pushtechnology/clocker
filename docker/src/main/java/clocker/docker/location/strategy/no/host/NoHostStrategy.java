/*
 * Copyright 2014-2016 by Cloudsoft Corporation Limited
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

package clocker.docker.location.strategy.no.host;

import org.apache.brooklyn.api.location.NoMachinesAvailableException;

import clocker.docker.entity.DockerHost;
import clocker.docker.entity.DockerInfrastructure;

/**
 * Strategy for handling provision requests when no host is available.
 */
public interface NoHostStrategy {
    /**
     * Handle a provisioning request when no host is available.
     * @return The docker host to use or {@link null}, if {@link null} is returned the placement strategies will be
     *      re-evaluated and this strategy may be called again
     * @throws NoMachinesAvailableException if thrown it will not re-evaluate the placement strategies and this
     *      strategy will not be called again
     */
    DockerHost noHosts(DockerInfrastructure dockerInfrastructure) throws NoMachinesAvailableException;
}
