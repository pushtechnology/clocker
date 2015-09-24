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

import brooklyn.location.docker.DockerLocation;

/**
 * Action to take when attempting to provision an entity in a docker cloud location and no hosts are available.
 * @author Matt Champion on 22/09/15
 */
public interface NoAvailableHostStrategy {

    /**
     * Perform an action when there are no hosts. This is only called when synchronized on the {@link DockerLocation}.
     * It should return true to indicate that another attempt to provision a container should go ahead. False will
     * result in the Docker location failing to provision a container. Until it returns false it may be repeatedly
     * called for the same entity.
     * @param location The location that has no hosts
     * @return {@code true} if there should be an attempt to obtain a host again
     */
    boolean handleNoHosts(DockerLocation location);
}
