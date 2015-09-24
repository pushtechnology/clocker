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
 * Increase the size of the Docker cloud if there is not free host. This is suitable when no autoscaler is being used.
 * @author Matt Champion on 22/09/15
 */
public final class ExpandOnNoHostStrategy implements NoAvailableHostStrategy {

    @Override
    public boolean handleNoHosts(DockerLocation location) {
        final int size = location.getOwner().getCurrentSize();
        location.getOwner().resize(size + 1);
        return true;
    }
}
