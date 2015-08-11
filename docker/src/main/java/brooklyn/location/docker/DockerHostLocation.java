/*
 * Copyright 2014-2015 by Cloudsoft Corporation Limited
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
package brooklyn.location.docker;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.config.ConfigKey;
import brooklyn.config.render.RendererHints;
import brooklyn.config.render.RendererHints.Hint;
import brooklyn.config.render.RendererHints.NamedActionWithUrl;
import brooklyn.entity.Entity;
import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.EntityAndAttribute;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.Lifecycle;
import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.container.DockerAttributes;
import brooklyn.entity.container.DockerCallbacks;
import brooklyn.entity.container.DockerUtils;
import brooklyn.entity.container.docker.DockerContainer;
import brooklyn.entity.container.docker.DockerHost;
import brooklyn.entity.container.docker.DockerInfrastructure;
import brooklyn.entity.group.DynamicCluster;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.PortAttributeSensorAndConfigKey;
import brooklyn.location.MachineProvisioningLocation;
import brooklyn.location.NoMachinesAvailableException;
import brooklyn.location.basic.AbstractLocation;
import brooklyn.location.basic.LocationConfigKeys;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.location.dynamic.DynamicLocation;
import brooklyn.location.jclouds.JcloudsLocation;
import brooklyn.networking.common.subnet.PortForwarder;
import brooklyn.networking.sdn.SdnAgent;
import brooklyn.networking.sdn.SdnAttributes;
import brooklyn.networking.sdn.SdnProvider;
import brooklyn.networking.subnet.SubnetTier;
import brooklyn.util.collections.MutableMap;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.flags.SetFromFlag;
import brooklyn.util.net.Cidr;
import brooklyn.util.ssh.BashCommands;
import brooklyn.util.text.Strings;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DockerHostLocation extends AbstractLocation implements MachineProvisioningLocation<DockerContainerLocation>, DockerVirtualLocation,
        DynamicLocation<DockerHost, DockerHostLocation>, Closeable {

    private static final long serialVersionUID = -1453203257759956820L;

    private static final Logger LOG = LoggerFactory.getLogger(DockerHostLocation.class);

    private transient final Lock lock = new ReentrantLock();

    @SetFromFlag("machine")
    private SshMachineLocation machine;

    @SetFromFlag("jcloudsLocation")
    private JcloudsLocation jcloudsLocation;

    @SetFromFlag("portForwarder")
    private PortForwarder portForwarder;

    @SetFromFlag("owner")
    private DockerHost dockerHost;

    @SetFromFlag("images")
    private ConcurrentMap<String, CountDownLatch> images = Maps.newConcurrentMap();

    public DockerHostLocation() {
        this(Maps.newLinkedHashMap());
    }

    public DockerHostLocation(Map properties) {
        super(properties);

        if (isLegacyConstruction()) {
            init();
        }
    }

    public DockerContainerLocation obtain() throws NoMachinesAvailableException {
        return obtain(Maps.<String,Object>newLinkedHashMap());
    }

    @Override
    public DockerContainerLocation obtain(Map<?,?> flags) throws NoMachinesAvailableException {
        while (!Lifecycle.RUNNING.equals(dockerHost.getAttribute(Attributes.SERVICE_STATE_ACTUAL))) {
            try {
                Thread.sleep(1000L);
            }
            catch (InterruptedException e) {
                throw new NoMachinesAvailableException("Interrupted while waiting for service to start", e);
            }
        }

        lock.lock();
        try {
            // Lookup entity from context or flags
            Object context = flags.get(LocationConfigKeys.CALLER_CONTEXT.getName());
            if (context == null || !(context instanceof Entity)) {
                throw new IllegalStateException("Invalid location context: " + context);
            }
            Entity entity = (Entity) context;

            // Configure the entity
            LOG.info("Configuring entity {} via subnet {}", entity, dockerHost.getSubnetTier());
            entity.config().set(SubnetTier.PORT_FORWARDING_MANAGER, dockerHost.getSubnetTier().getPortForwardManager());
            entity.config().set(SubnetTier.PORT_FORWARDER, portForwarder);
            if (getOwner().config().get(SdnAttributes.SDN_ENABLE)) {
                SdnAgent agent = getOwner().getAttribute(SdnAgent.SDN_AGENT);
                if (agent == null) {
                    throw new IllegalStateException("SDN agent entity on " + getOwner() + " is null");
                }
                Map<String, Cidr> networks = agent.getAttribute(SdnAgent.SDN_PROVIDER).getAttribute(SdnProvider.SUBNETS);
                entity.config().set(SubnetTier.SUBNET_CIDR, networks.get(entity.getApplicationId()));
            } else {
                entity.config().set(SubnetTier.SUBNET_CIDR, Cidr.UNIVERSAL);
            }
            configureEnrichers(entity);

            // Add the entity Dockerfile if configured
            String dockerfile = entity.config().get(DockerAttributes.DOCKERFILE_URL);
            String imageId = entity.config().get(DockerAttributes.DOCKER_IMAGE_ID);

            Optional<String> baseImage = Optional.fromNullable(entity.config().get(DockerAttributes.DOCKER_IMAGE_NAME));
            String imageTag = Optional.fromNullable(entity.config().get(DockerAttributes.DOCKER_IMAGE_TAG)).or("latest");
            // TODO incorporate more info
            final String imageName = DockerUtils.imageName(entity, dockerfile);

            // Lookup image ID or build new image from Dockerfile
            LOG.info("ImageName for entity {}: {}", entity, imageName);

            if (dockerHost.getImageNamed(imageName, imageTag).isPresent()) {
                // Wait until committed before continuing - Brooklyn may be midway through its creation.
                waitForImage(imageName);

                // Look up imageId again
                imageId = dockerHost.getImageNamed(imageName, imageTag).get();
                LOG.info("Found image {} for entity: {}", imageName, imageId);

                // Skip install phase
                entity.config().set(SoftwareProcess.SKIP_INSTALLATION, true);
            } else if (baseImage.isPresent()) {
                // Create an SSHable image from the one configured
                imageId = dockerHost.layerSshableImageOn(baseImage.get(), imageTag);
                LOG.info("Created SSHable image from {}: {}", baseImage.get(), imageId);
                entity.config().set(SoftwareProcess.SKIP_INSTALLATION, true);
            } else {
                // Create latch for image name
                images.putIfAbsent(imageName, new CountDownLatch(1));

                // Otherwise Clocker is going to make an image for the entity once it is installed.
                insertCallback(entity, SoftwareProcess.POST_INSTALL_COMMAND, DockerCallbacks.commit());

                if (Strings.isNonBlank(dockerfile)) {
                    if (imageId != null) {
                        LOG.warn("Ignoring container imageId {} as dockerfile URL is set: {}", imageId, dockerfile);
                    }
                    imageId = dockerHost.createSshableImage(dockerfile, imageName);
                }
                if (Strings.isBlank(imageId)) {
                    imageId = getOwner().getAttribute(DockerHost.DOCKER_IMAGE_ID);
                }

                // Tag image name
                dockerHost.runDockerCommand(String.format("tag -f %s %s:latest", imageId, imageName));
            }

            // Set subnet address pre install
            insertCallback(entity, SoftwareProcess.PRE_INSTALL_COMMAND, DockerCallbacks.subnetAddress());

            // Look up hardware ID
            String hardwareId = entity.config().get(DockerAttributes.DOCKER_HARDWARE_ID);
            if (Strings.isEmpty(hardwareId)) {
                hardwareId = getOwner().config().get(DockerAttributes.DOCKER_HARDWARE_ID);
            }

            // Create new Docker container in the host cluster
            LOG.info("Starting container with imageId {} and hardwareId {} at {}", new Object[] { imageId, hardwareId, machine });
            Map<Object, Object> containerFlags = MutableMap.builder()
                    .putAll(flags)
                    .put("entity", entity)
                    .putIfNotNull("imageId", imageId)
                    .putIfNotNull("hardwareId", hardwareId)
                    .build();
            DynamicCluster cluster = dockerHost.getDockerContainerCluster();
            Entity added = cluster.addNode(machine, containerFlags);
            if (added == null) {
                throw new NoMachinesAvailableException(String.format("Failed to create container at %s", dockerHost.getDockerHostName()));
            } else {
                Entities.start(added, ImmutableList.of(machine));
            }
            DockerContainer dockerContainer = (DockerContainer) added;

            // Save the container attributes
            ((EntityLocal) dockerContainer).setAttribute(DockerContainer.IMAGE_ID, imageId);
            ((EntityLocal) dockerContainer).setAttribute(DockerContainer.IMAGE_NAME, imageName);
            ((EntityLocal) dockerContainer).setAttribute(DockerContainer.HARDWARE_ID, hardwareId);

            // record SDN application network details
            if (getOwner().config().get(SdnAttributes.SDN_ENABLE)) {
                SdnAgent agent = getOwner().getAttribute(SdnAgent.SDN_AGENT);
                Cidr applicationCidr =  agent.getAttribute(SdnAgent.SDN_PROVIDER).getSubnetCidr(entity.getApplicationId());
                ((EntityLocal) entity).setAttribute(SdnProvider.APPLICATION_CIDR, applicationCidr);
                ((EntityLocal) dockerContainer).setAttribute(SdnProvider.APPLICATION_CIDR, applicationCidr);
            }

            return dockerContainer.getDynamicLocation();
        } finally {
            lock.unlock();
        }
    }

    private void insertCallback(Entity entity, ConfigKey<String> commandKey, String callback) {
        String command = entity.config().get(commandKey);
        if (Strings.isNonBlank(command)) {
            command = BashCommands.chain(command, callback);
        } else {
            command = callback;
        }
        entity.config().set(commandKey, command);
    }

    public void waitForImage(String imageName) {
        try {
            CountDownLatch latch = images.get(imageName);
            if (latch != null) latch.await(15, TimeUnit.MINUTES);
        } catch (InterruptedException ie) {
            throw Exceptions.propagate(ie);
        }
    }

    public void markImage(String imageName) {
        CountDownLatch latch = images.get(imageName);
        if (latch != null) latch.countDown();
    }

    private void configureEnrichers(Entity entity) {
        for (AttributeSensor sensor : Iterables.filter(entity.getEntityType().getSensors(), AttributeSensor.class)) {
            if ((DockerUtils.URL_SENSOR_NAMES.contains(sensor.getName()) ||
                        sensor.getName().endsWith(".url") ||
                        URI.class.isAssignableFrom(sensor.getType())) &&
                    !DockerUtils.BLACKLIST_URL_SENSOR_NAMES.contains(sensor.getName())) {
                AttributeSensor<String> target = DockerUtils.<String>mappedSensor(sensor);
                entity.addEnricher(dockerHost.getSubnetTier().uriTransformingEnricher(
                        EntityAndAttribute.create(entity, sensor), target));
                Set<Hint<?>> hints = RendererHints.getHintsFor(sensor);
                for (Hint<?> hint : hints) {
                    RendererHints.register(target, (NamedActionWithUrl) hint);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Mapped URL sensor: origin={}, mapped={}", sensor.getName(), target.getName());
                }
            } else if (PortAttributeSensorAndConfigKey.class.isAssignableFrom(sensor.getClass())) {
                AttributeSensor<String> target = DockerUtils.mappedPortSensor((PortAttributeSensorAndConfigKey) sensor);
                entity.addEnricher(dockerHost.getSubnetTier().hostAndPortTransformingEnricher(
                        EntityAndAttribute.create(entity, sensor), target));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Mapped port sensor: origin={}, mapped={}", sensor.getName(), target.getName());
                }
            }
        }
    }

    @Override
    public void release(DockerContainerLocation machine) {
        lock.lock();
        try {
            LOG.info("Releasing {}", machine);

            DynamicCluster cluster = dockerHost.getDockerContainerCluster();
            DockerContainer container = machine.getOwner();
            if (cluster.removeMember(container)) {
                LOG.info("Docker Host {}: member {} released", dockerHost.getDockerHostName(), machine);
            } else {
                LOG.warn("Docker Host {}: member {} not found for release", dockerHost.getDockerHostName(), machine);
            }

            // Now close and unmange the container
            try {
                machine.close();
                container.stop();
            } catch (Exception e) {
                LOG.warn("Error stopping container: " + container, e);
                Exceptions.propagateIfFatal(e);
            } finally {
                Entities.unmanage(container);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<String, Object> getProvisioningFlags(Collection<String> tags) {
        return MutableMap.of();
    }

    @Override
    public DockerHost getOwner() {
        return dockerHost;
    }

    public SshMachineLocation getMachine() {
        return machine;
    }

    public JcloudsLocation getJcloudsLocation() {
        return jcloudsLocation;
    }

    public PortForwarder getPortForwarder() {
        return portForwarder;
    }

    public int getCurrentSize() {
        return dockerHost.getCurrentSize();
    }

    @Override
    public MachineProvisioningLocation<DockerContainerLocation> newSubLocation(Map<?, ?> newFlags) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Entity> getDockerContainerList() {
        return dockerHost.getDockerContainerList();
    }

    @Override
    public List<Entity> getDockerHostList() {
        return Lists.<Entity>newArrayList(dockerHost);
    }

    @Override
    public DockerInfrastructure getDockerInfrastructure() {
        return ((DockerLocation) getParent()).getDockerInfrastructure();
    }

    @Override
    public void close() throws IOException {
        LOG.info("Close called on Docker host {}: {}", machine, this);
        try {
            machine.close();
        } catch (Exception e) {
            LOG.info("{}: Closing Docker host: {}", e.getMessage(), this);
            throw Exceptions.propagate(e);
        } finally {
            LOG.info("Docker host closed: {}", this);
        }
    }

    public Lock getLock() {
        return lock;
    }

    @Override
    public ToStringHelper string() {
        return super.string()
                .add("machine", machine)
                .add("jcloudsLocation", jcloudsLocation)
                .add("dockerHost", dockerHost);
    }

}
