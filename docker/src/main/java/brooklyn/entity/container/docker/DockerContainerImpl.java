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

import static java.lang.String.format;

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.jclouds.compute.domain.Processor;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.docker.compute.options.DockerTemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.config.ConfigKey;
import brooklyn.config.render.RendererHints;
import brooklyn.entity.Entity;
import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.BasicStartableImpl;
import brooklyn.entity.basic.DelegateEntity;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.Lifecycle;
import brooklyn.entity.basic.ServiceStateLogic;
import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.container.DockerAttributes;
import brooklyn.entity.container.weave.WeaveContainer;
import brooklyn.event.feed.ConfigToAttributes;
import brooklyn.event.feed.function.FunctionFeed;
import brooklyn.event.feed.function.FunctionPollConfig;
import brooklyn.location.Location;
import brooklyn.location.LocationSpec;
import brooklyn.location.NoMachinesAvailableException;
import brooklyn.location.PortRange;
import brooklyn.location.basic.LocationConfigKeys;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.location.cloud.CloudLocationConfig;
import brooklyn.location.docker.DockerContainerLocation;
import brooklyn.location.docker.DockerHostLocation;
import brooklyn.location.dynamic.DynamicLocation;
import brooklyn.location.jclouds.JcloudsLocation;
import brooklyn.location.jclouds.JcloudsLocationConfig;
import brooklyn.location.jclouds.JcloudsSshMachineLocation;
import brooklyn.location.jclouds.templates.PortableTemplateBuilder;
import brooklyn.management.LocationManager;
import brooklyn.networking.portforwarding.subnet.JcloudsPortforwardingSubnetLocation;
import brooklyn.networking.subnet.SubnetTier;
import brooklyn.util.collections.MutableMap;
import brooklyn.util.collections.MutableSet;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.internal.ssh.SshTool;
import brooklyn.util.net.Cidr;
import brooklyn.util.net.Urls;
import brooklyn.util.text.Strings;
import brooklyn.util.time.Duration;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;

/**
 * A single Docker container.
 */
public class DockerContainerImpl extends BasicStartableImpl implements DockerContainer {

    private static final Logger LOG = LoggerFactory.getLogger(DockerContainerImpl.class);
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    static {
        RendererHints.register(DOCKER_HOST, new RendererHints.NamedActionWithUrl("Open", DelegateEntity.EntityUrl.entityUrl()));
        RendererHints.register(ENTITY, new RendererHints.NamedActionWithUrl("Open", DelegateEntity.EntityUrl.entityUrl()));
        RendererHints.register(CONTAINER, new RendererHints.NamedActionWithUrl("Open", DelegateEntity.EntityUrl.entityUrl()));
    }

    private transient FunctionFeed sensorFeed;

    @Override
    public void init() {
        LOG.info("Starting Docker container id {}", getId());
        super.init();

        String dockerContainerName = format(getConfig(DockerContainer.DOCKER_CONTAINER_NAME_FORMAT), getId(), COUNTER.incrementAndGet());
        setDisplayName(dockerContainerName);
        setAttribute(DOCKER_CONTAINER_NAME, dockerContainerName);

        ConfigToAttributes.apply(this, DOCKER_INFRASTRUCTURE);
        ConfigToAttributes.apply(this, DOCKER_HOST);
        ConfigToAttributes.apply(this, ENTITY);
    }

    protected void connectSensors() {
        sensorFeed = FunctionFeed.builder()
                .entity(this)
                .period(Duration.TEN_SECONDS)
                .poll(new FunctionPollConfig<Boolean, Boolean>(SERVICE_UP)
                        .callable(new Callable<Boolean>() {
                                @Override
                                public Boolean call() throws Exception {
                                    return getAttribute(SSH_MACHINE_LOCATION).isSshable();
                                }
                        })
                        .onFailureOrException(Functions.constant(Boolean.FALSE)))
                .poll(new FunctionPollConfig<Boolean, Boolean>(CONTAINER_RUNNING)
                        .callable(new Callable<Boolean>() {
                                @Override
                                public Boolean call() throws Exception {
                                    String running = getDockerHost().runDockerCommand("inspect -f {{.State.Running}} " + getContainerId());
                                    return Boolean.parseBoolean(Strings.trim(running));
                                }
                        })
                        .onFailureOrException(Functions.constant(Boolean.FALSE)))
                .build();
    }

    public void disconnectSensors() {
        if (sensorFeed !=  null) sensorFeed.stop();
    }

    @Override
    public Entity getRunningEntity() {
        return getAttribute(ENTITY);
    }

    public void setRunningEntity(Entity entity) {
        setAttribute(ENTITY, entity);
    }

    @Override
    public String getDockerContainerName() {
        return getAttribute(DOCKER_CONTAINER_NAME);
    }

    @Override
    public String getContainerId() {
        return getAttribute(CONTAINER_ID);
    }

    @Override
    public SshMachineLocation getMachine() {
        return getAttribute(SSH_MACHINE_LOCATION);
    }

    @Override
    public DockerHost getDockerHost() {
        return getConfig(DOCKER_HOST);
    }

    @Override
    public String getShortName() {
        return "Docker Container";
    }

    @Override
    public DockerContainerLocation getDynamicLocation() {
        return (DockerContainerLocation) getAttribute(DYNAMIC_LOCATION);
    }

    @Override
    public boolean isLocationAvailable() {
        return getDynamicLocation() != null;
    }

    @Override
    public void startUp() {
        String dockerContainerName = getAttribute(DockerContainer.DOCKER_CONTAINER_NAME);
        LOG.info("Start-Up {}", dockerContainerName);
        getDockerHost().runDockerCommand("start " + getContainerId());
    }

    @Override
    public void shutDown() {
        String dockerContainerName = getAttribute(DockerContainer.DOCKER_CONTAINER_NAME);
        LOG.info("Shut-Down {}", dockerContainerName);
        getDockerHost().runDockerCommand("stop " + getContainerId());
    }

    @Override
    public void pause() {
        String dockerContainerName = getAttribute(DockerContainer.DOCKER_CONTAINER_NAME);
        LOG.info("Pausing {}", dockerContainerName);
        getDockerHost().runDockerCommand("pause " + getContainerId());
    }

    @Override
    public void resume() {
        String dockerContainerName = getAttribute(DockerContainer.DOCKER_CONTAINER_NAME);
        LOG.info("Resume {}", dockerContainerName);
        getDockerHost().runDockerCommand("unpause " + getContainerId());
    }

    private DockerTemplateOptions getDockerTemplateOptions() {
        Entity entity = getRunningEntity();
        DockerTemplateOptions options = DockerTemplateOptions.NONE;

        // Use DockerHost hostname for the container
        Boolean useHostDns = entity.getConfig(DOCKER_USE_HOST_DNS_NAME);
        if (useHostDns == null) useHostDns = getConfig(DOCKER_USE_HOST_DNS_NAME);
        if (useHostDns != null && useHostDns) {
            // FIXME does not seem to work on Softlayer, should set HOSTNAME or SUBNET_HOSTNAME
            String hostname = getDockerHost().getAttribute(Attributes.HOSTNAME);
            String address = getDockerHost().getAttribute(Attributes.ADDRESS);
            if (hostname.equalsIgnoreCase(address)) {
                options.hostname(getDockerContainerName());
            } else {
                options.hostname(hostname);
            }
        }

        // CPU shares
        Integer cpuShares = entity.getConfig(DOCKER_CPU_SHARES);
        if (cpuShares == null) cpuShares = getConfig(DOCKER_CPU_SHARES);
        if (cpuShares != null) {
            // TODO set based on number of cores available in host divided by cores requested in flags
            Integer hostCores = (int) getDockerHost().getDynamicLocation().getMachine().getMachineDetails().getHardwareDetails().getCpuCount();
            Integer minCores = (Integer) entity.getConfig(JcloudsLocationConfig.MIN_CORES);
            Map flags = entity.getConfig(SoftwareProcess.PROVISIONING_PROPERTIES);
            if (minCores == null && flags != null) {
                minCores = (Integer) flags.get(JcloudsLocationConfig.MIN_CORES.getName());
            }
            if (minCores == null && flags != null) {
                TemplateBuilder template = (TemplateBuilder) flags.get(JcloudsLocationConfig.TEMPLATE_BUILDER.getName());
                if (template != null) {
                    minCores = 0;
                    for (Processor cpu : template.build().getHardware().getProcessors()) {
                        minCores = minCores + (int) cpu.getCores();
                    }
                }
            }
            if (minCores != null) {
                double ratio = (double) minCores / (double) hostCores;
                LOG.info("Cores: host {}, min {}, ratio {}", new Object[] { hostCores, minCores, ratio });
            }
        }
        if (cpuShares != null) options.cpuShares(cpuShares);

        // Memory
        Integer memory = entity.getConfig(DOCKER_MEMORY);
        if (memory == null) memory = getConfig(DOCKER_MEMORY);
        if (memory != null) {
            // TODO set based on memory available in host divided by memory requested in flags
            Integer hostRam = getDockerHost().getDynamicLocation().getMachine().getMachineDetails().getHardwareDetails().getRam();
            Integer minRam = entity.getConfig(JcloudsLocationConfig.MIN_RAM);
            Map flags = entity.getConfig(SoftwareProcess.PROVISIONING_PROPERTIES);
            if (minRam == null && flags != null) {
                minRam = (Integer) flags.get(JcloudsLocationConfig.MIN_RAM.getName());
            }
            if (minRam == null && flags != null) {
                TemplateBuilder template = (TemplateBuilder) flags.get(JcloudsLocationConfig.TEMPLATE_BUILDER.getName());
                if (template != null) {
                    minRam = template.build().getHardware().getRam();
                }
            }
            if (minRam != null) {
                double ratio = (double) minRam / (double) hostRam;
                LOG.info("Memory: host {}, min {}, ratio {}", new Object[] { hostRam, minRam, ratio });
            }
        }
        if (memory != null) options.memory(memory);

        // Volumes
        Map<String, String> volumes = MutableMap.copyOf(getDockerHost().getAttribute(DockerHost.DOCKER_HOST_VOLUME_MAPPING));
        Map<String, String> mapping = entity.getConfig(DockerHost.DOCKER_HOST_VOLUME_MAPPING);
        if (mapping != null) {
            for (String source : mapping.keySet()) {
                if (Urls.isUrlWithProtocol(source)) {
                    String path = getDockerHost().deployArchive(source);
                    volumes.put(path, mapping.get(source));
                } else {
                    volumes.put(source, mapping.get(source));
                }
            }
        }
        List<String> exports = entity.getConfig(DockerContainer.DOCKER_CONTAINER_VOLUME_EXPORT);
        if (exports != null) {
            for (String dir : exports) {
                volumes.put(dir, dir);
            }
        }
        options.volumes(volumes);

        // Set login password from the Docker host
        options.overrideLoginPassword(getDockerHost().getPassword());

        // Look for environment variables configured on the entity
        Map<String, Object> shellEnv = entity.getConfig(SoftwareProcess.SHELL_ENVIRONMENT);
        if (shellEnv != null && !shellEnv.isEmpty()) {
            String env = Joiner.on(':').withKeyValueSeparator("=").join(shellEnv);
            options.env(ImmutableList.of(env));
        }

        return options;
    }

    /**
     * Create a new {@link DockerContainerLocation} wrapping a machine from the host's {@link JcloudsLocation}.
     */
    @Override
    public DockerContainerLocation createLocation(Map flags) {
        DockerHost dockerHost = getDockerHost();
        DockerHostLocation host = dockerHost.getDynamicLocation();
        SubnetTier subnetTier = dockerHost.getSubnetTier();

        // Configure the container options based on the host and the running entity
        DockerTemplateOptions options = getDockerTemplateOptions();

        // put these fields on the location so it has the info it needs to create the subnet
        Map<?, ?> dockerFlags = MutableMap.<Object, Object>builder()
                .put(JcloudsLocationConfig.TEMPLATE_BUILDER, new PortableTemplateBuilder().options(options))
                .put(JcloudsLocationConfig.IMAGE_ID, getConfig(DOCKER_IMAGE_ID))
                .put(JcloudsLocationConfig.HARDWARE_ID, getConfig(DOCKER_HARDWARE_ID))
                .put(LocationConfigKeys.USER, "root")
                .put(LocationConfigKeys.PASSWORD, getConfig(DOCKER_PASSWORD))
                .put(SshTool.PROP_PASSWORD, getConfig(DOCKER_PASSWORD))
                .put(LocationConfigKeys.PRIVATE_KEY_DATA, null)
                .put(LocationConfigKeys.PRIVATE_KEY_FILE, null)
                .put(CloudLocationConfig.WAIT_FOR_SSHABLE, false)
                .put(JcloudsLocationConfig.INBOUND_PORTS, getRequiredOpenPorts(getRunningEntity()))
                .put(JcloudsLocation.USE_PORT_FORWARDING, true)
                .put(JcloudsLocation.PORT_FORWARDER, subnetTier.getPortForwarderExtension())
                .put(JcloudsLocation.PORT_FORWARDING_MANAGER, subnetTier.getPortForwardManager())
                .put(JcloudsPortforwardingSubnetLocation.PORT_FORWARDER, subnetTier.getPortForwarder())
                .put(SubnetTier.SUBNET_CIDR, Cidr.CLASS_B)
                .build();

        try {
            // Create a new container using jclouds Docker driver
            JcloudsSshMachineLocation container = host.getJcloudsLocation().obtain(dockerFlags);
            setAttribute(CONTAINER_ID, container.getNode().getId());

            // If Weave is enabled, attach to the network
            if (getConfig(DockerInfrastructure.WEAVE_ENABLED)) {
                WeaveContainer weave = Entities.attributeSupplierWhenReady(dockerHost, WeaveContainer.WEAVE_CONTAINER).get();
                InetAddress subnetAddress = weave.attachNetwork(getAttribute(CONTAINER_ID));
                setAttribute(Attributes.SUBNET_ADDRESS, subnetAddress.getHostAddress());
            }

            // Create our wrapper location around the container
            LocationSpec<DockerContainerLocation> spec = LocationSpec.create(DockerContainerLocation.class)
                    .parent(host)
                    .configure(flags)
                    .configure(DynamicLocation.OWNER, this)
                    .configure("machine", container) // the underlying JcloudsLocation
                    .configure(container.getAllConfig(true))
                    .displayName(getDockerContainerName());
            DockerContainerLocation location = getManagementContext().getLocationManager().createLocation(spec);

            setAttribute(DYNAMIC_LOCATION, location);
            setAttribute(LOCATION_NAME, location.getId());

            LOG.info("New Docker container location {} created", location);
            return location;
        } catch (NoMachinesAvailableException e) {
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public void deleteLocation() {
        DockerContainerLocation location = getDynamicLocation();

        if (location != null) {
            LocationManager mgr = getManagementContext().getLocationManager();
            if (mgr.isManaged(location)) {
                mgr.unmanage(location);
            }
        }

        setAttribute(DYNAMIC_LOCATION, null);
        setAttribute(LOCATION_NAME, null);
    }

    /** @return the ports required for a specific entity */
    protected Collection<Integer> getRequiredOpenPorts(Entity entity) {
        Set<Integer> ports = MutableSet.of(22);
        for (ConfigKey<?> k: entity.getEntityType().getConfigKeys()) {
            if (PortRange.class.isAssignableFrom(k.getType())) {
                PortRange p = (PortRange) entity.getConfig(k);
                if (p != null && !p.isEmpty()) ports.add(p.iterator().next());
            }
        }
        LOG.debug("getRequiredOpenPorts detected default {} for {}", ports, entity);
        return ports;
    }

    @Override
    public void start(Collection<? extends Location> locations) {
        ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);
        setAttribute(SoftwareProcess.SERVICE_UP, Boolean.FALSE);

        Boolean started = getConfig(SoftwareProcess.ENTITY_STARTED);
        if (Boolean.TRUE.equals(started)) {
            DockerHost dockerHost = getDockerHost();
            DockerHostLocation host = dockerHost.getDynamicLocation();
            setAttribute(DockerContainer.IMAGE_ID, getConfig(DOCKER_IMAGE_ID));
            setAttribute(DockerContainer.IMAGE_NAME, getConfig(DockerAttributes.DOCKER_IMAGE_NAME));
            setAttribute(SSH_MACHINE_LOCATION, host.getMachine());
        } else {
            Map<String, ?> flags = MutableMap.copyOf(getConfig(LOCATION_FLAGS));
            DockerContainerLocation location = createLocation(flags);
            setAttribute(SSH_MACHINE_LOCATION, location.getMachine());
        }

        connectSensors();

        super.start(locations);

        ServiceStateLogic.setExpectedState(this, Lifecycle.RUNNING);
    }

    @Override
    public void stop() {
        ServiceStateLogic.setExpectedState(this, Lifecycle.STOPPING);

        super.stop();

        disconnectSensors();

        setAttribute(SSH_MACHINE_LOCATION, null);

        Boolean started = getConfig(SoftwareProcess.ENTITY_STARTED);
        if (Boolean.TRUE.equals(started)) {
            deleteLocation();
        }

        setAttribute(SoftwareProcess.SERVICE_UP, Boolean.FALSE);
        ServiceStateLogic.setExpectedState(this, Lifecycle.STOPPED);
    }

}
