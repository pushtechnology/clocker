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

import java.util.List;

import brooklyn.config.ConfigKey;
import brooklyn.entity.Entity;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.basic.BasicStartable;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.Lifecycle;
import brooklyn.entity.basic.MethodEffector;
import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.container.DockerAttributes;
import brooklyn.entity.container.DockerUtils;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.entity.trait.HasShortName;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.AttributeSensorAndConfigKey;
import brooklyn.event.basic.Sensors;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.location.docker.DockerContainerLocation;
import brooklyn.location.dynamic.LocationOwner;
import brooklyn.location.jclouds.JcloudsSshMachineLocation;
import brooklyn.util.flags.SetFromFlag;

/**
 * A Docker container.
 * <p>
 * This entity controls the {@link DockerContainerLocation} location, and creates
 * and the {@link JcloudsSshMachineLocation} that entities communicate with when
 * deployed to the {@link DockerInfrastructure}.
 */
@ImplementedBy(DockerContainerImpl.class)
public interface DockerContainer extends BasicStartable, HasShortName, LocationOwner<DockerContainerLocation, DockerContainer> {

    @SetFromFlag("dockerHost")
    AttributeSensorAndConfigKey<DockerHost, DockerHost> DOCKER_HOST = ConfigKeys.newSensorAndConfigKey(DockerHost.class, "docker.host", "The parent Docker host");

    @SetFromFlag("infrastructure")
    AttributeSensorAndConfigKey<DockerInfrastructure, DockerInfrastructure> DOCKER_INFRASTRUCTURE = DockerHost.DOCKER_INFRASTRUCTURE;

    @SetFromFlag("managed")
    ConfigKey<Boolean> MANAGED = DockerAttributes.MANAGED;

    @SetFromFlag("password")
    ConfigKey<String> DOCKER_PASSWORD = DockerAttributes.DOCKER_PASSWORD;

    @SetFromFlag("imageId")
    ConfigKey<String> DOCKER_IMAGE_ID = DockerAttributes.DOCKER_IMAGE_ID.getConfigKey();

    @SetFromFlag("hardwareId")
    ConfigKey<String> DOCKER_HARDWARE_ID = DockerAttributes.DOCKER_HARDWARE_ID.getConfigKey();

    @SetFromFlag("useHostDnsName")
    ConfigKey<Boolean> DOCKER_USE_HOST_DNS_NAME = DockerAttributes.DOCKER_USE_HOST_DNS_NAME;

    @SetFromFlag("cpuShares")
    ConfigKey<Integer> DOCKER_CPU_SHARES = DockerAttributes.DOCKER_CPU_SHARES;

    @SetFromFlag("memory")
    ConfigKey<Integer> DOCKER_MEMORY = DockerAttributes.DOCKER_MEMORY;

    @SetFromFlag("volumeExports")
    ConfigKey<List<String>> DOCKER_CONTAINER_VOLUME_EXPORT = DockerAttributes.DOCKER_CONTAINER_VOLUME_EXPORT;

    @SetFromFlag("entity")
    AttributeSensorAndConfigKey<Entity, Entity> ENTITY = ConfigKeys.newSensorAndConfigKey(Entity.class, "docker.container.entity", "The entity running in this Docker container");

    ConfigKey<String> DOCKER_CONTAINER_NAME_FORMAT = ConfigKeys.newStringConfigKey("docker.container.nameFormat",
            "Format for generating Docker container names", DockerUtils.DEFAULT_DOCKER_CONTAINER_NAME_FORMAT);

    AttributeSensor<String> DOCKER_CONTAINER_NAME = Sensors.newStringSensor("docker.container.name", "The name of the Docker container");

    AttributeSensor<String> IMAGE_ID = Sensors.newStringSensor("docker.container.imageId", "The Docker container image ID");
    AttributeSensor<String> IMAGE_NAME = Sensors.newStringSensor("docker.container.imageName", "The Docker container image name");
    AttributeSensor<String> HARDWARE_ID = Sensors.newStringSensor("docker.container.hardwareId", "The Docker container hardware ID");
    AttributeSensor<String> CONTAINER_ID = Sensors.newStringSensor("docker.container.id", "The Docker container ID");

    AttributeSensor<Entity> CONTAINER = Sensors.newSensor(Entity.class, "docker.container", "The Docker container entity");

    AttributeSensor<Boolean> CONTAINER_RUNNING = Sensors.newBooleanSensor("docker.container.running", "The Docker container process status");

    AttributeSensor<Lifecycle> SERVICE_STATE_ACTUAL = SoftwareProcess.SERVICE_STATE_ACTUAL;

    AttributeSensor<SshMachineLocation> SSH_MACHINE_LOCATION = Sensors.newSensor(SshMachineLocation.class, "docker.container.ssh", "The SSHable machine");

    MethodEffector<Void> SHUT_DOWN = new MethodEffector<Void>(DockerContainer.class, "shutDown");
    MethodEffector<Void> PAUSE = new MethodEffector<Void>(DockerContainer.class, "pause");
    MethodEffector<Void> RESUME = new MethodEffector<Void>(DockerContainer.class, "resume");

    /**
     * Start-up the Docker container. Invokes docker start.
     */
    @Effector(description="Start-up the Docker container")
    void startUp();

    /**
     * Shut-down the Docker container. Invokes docker stop.
     */
    @Effector(description="Shut-down the Docker container")
    void shutDown();

    /**
     * Pause the Docker container. Invokes docker pause.
     */
    @Effector(description="Pause the Docker container")
    void pause();

    /**
     * Resume the Docker container. Invokes docker unpause.
     */
    @Effector(description="Resume the Docker container")
    void resume();

    String getDockerContainerName();

    String getContainerId();

    Entity getRunningEntity();

    void setRunningEntity(Entity entity);

    DockerHost getDockerHost();

    SshMachineLocation getMachine();

}
