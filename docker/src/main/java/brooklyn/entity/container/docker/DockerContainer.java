/*
 * Copyright 2014 by Cloudsoft Corporation Limited
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
package brooklyn.entity.container.docker;

import brooklyn.config.ConfigKey;
import brooklyn.entity.Entity;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.basic.BasicStartable;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.Lifecycle;
import brooklyn.entity.basic.MethodEffector;
import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.entity.trait.HasShortName;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.Sensors;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.location.docker.DockerContainerLocation;
import brooklyn.location.dynamic.LocationOwner;
import brooklyn.location.jclouds.JcloudsLocationConfig;
import brooklyn.util.flags.SetFromFlag;

@ImplementedBy(DockerContainerImpl.class)
public interface DockerContainer extends BasicStartable, HasShortName, LocationOwner<DockerContainerLocation, DockerContainer> {

    String DEFAULT_DOCKER_CONTAINER_NAME_FORMAT = "docker-container-brooklyn-%1$s";

    @SetFromFlag("dockerHost")
    ConfigKey<DockerHost> DOCKER_HOST = ConfigKeys.newConfigKey(DockerHost.class, "docker.host", "The parent Docker host");

    @SetFromFlag("imageNameRegex")
    ConfigKey<String> DOCKER_IMAGE_NAME_REGEX = JcloudsLocationConfig.IMAGE_NAME_REGEX;

    ConfigKey<String> DOCKER_CONTAINER_NAME_FORMAT = ConfigKeys.newStringConfigKey("docker.container.nameFormat",
            "Format for generating Docker container names", DEFAULT_DOCKER_CONTAINER_NAME_FORMAT);

    AttributeSensor<String> DOCKER_CONTAINER_NAME = Sensors.newStringSensor("docker.container.name", "The name of the Docker container");

    AttributeSensor<Lifecycle> SERVICE_STATE = SoftwareProcess.SERVICE_STATE;

    AttributeSensor<SshMachineLocation> SSH_MACHINE_LOCATION = Sensors.newSensor(SshMachineLocation.class, "docker.container.ssh", "The SSHable machine");

    AttributeSensor<Entity> ENTITY = Sensors.newSensor(Entity.class, "docker.container.entity", "The entity running in this Docker container");

    MethodEffector<Void> SHUT_DOWN = new MethodEffector<Void>(DockerContainer.class, "shutDown");
    MethodEffector<Void> PAUSE = new MethodEffector<Void>(DockerContainer.class, "pause");
    MethodEffector<Void> RESUME = new MethodEffector<Void>(DockerContainer.class, "resume");

    /**
     * Shut-down the Docker container.
     */
    @Effector(description="Shut-down the Docker container")
    void shutDown();

    /**
     * Pause the Docker container.
     */
    @Effector(description="Pause the Docker container")
    void pause();

    /**
     * Resume the Docker container.
     */
    @Effector(description="Resume the Docker container")
    void resume();

    String getDockerContainerName();

    Entity getRunningEntity();
    void setRunningEntity(Entity entity);

    DockerHost getDockerHost();

}
