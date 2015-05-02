package brooklyn.location.docker.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.container.DockerAttributes;
import brooklyn.entity.container.docker.DockerHost;
import brooklyn.entity.container.docker.DockerInfrastructure;
import brooklyn.location.Location;
import brooklyn.location.NoMachinesAvailableException;
import brooklyn.location.docker.DockerLocation;
import brooklyn.policy.basic.AbstractPolicy;
import com.google.common.collect.Iterables;

/**
 * Increase the size of the Docker cloud.
 * @author Matt Champion on 24/04/15
 */
public final class ExpandOnNoHostStrategy extends AbstractPolicy implements NoAvailableHostStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(DockerLocation.class);
    private final transient Semaphore permit = new Semaphore(1);
    private volatile Location provisioner;
    private volatile List<DockerAwarePlacementStrategy> strategies;

    public ExpandOnNoHostStrategy() {
    }

    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);

        provisioner = Iterables.getOnlyElement(entity.getLocations());
        final List<DockerAwarePlacementStrategy> infrastructureStrategies = entity.config()
            .get(DockerInfrastructure.PLACEMENT_STRATEGIES);
        if (strategies != null) {
            strategies = infrastructureStrategies;
        }
        else {
            strategies = new ArrayList<DockerAwarePlacementStrategy>();
        }
    }

    @Override
    public DockerHost handleNoHosts(DockerLocation location, Entity newEntity, Map<?, ?> flags) throws NoMachinesAvailableException {
        // Get permission to create a new Docker host
        try {
            permit.acquire();
            return provisionNewHost(newEntity, flags);
        }
        catch (Throwable t) {
            throw new NoMachinesAvailableException("Exception while provisioning new machine", t);
        }
        finally {
            permit.release();
        }
    }

    private DockerHost provisionNewHost(Entity newEntity, Map<?, ?> flags) {
        final List<DockerAwarePlacementStrategy> entityStrategies = newEntity.config().get(DockerAttributes.PLACEMENT_STRATEGIES);
        final Iterable<DockerAwarePlacementStrategy> allStrategies;
        if (entityStrategies == null) {
            allStrategies = strategies;
        }
        else {
            allStrategies = Iterables.concat(strategies, entityStrategies);
        }
        final Iterable<DockerAwareProvisioningStrategy> provisioningStrategies = Iterables.filter(allStrategies, DockerAwareProvisioningStrategy.class);
        for (final DockerAwareProvisioningStrategy strategy : provisioningStrategies) {
            flags = strategy.apply((Map<String,Object>) flags);
        }

        LOG.info("Provisioning new host with flags: {}", flags);
        final DockerInfrastructure infrastructure = (DockerInfrastructure)entity;
        return (DockerHost) infrastructure.getDockerHostCluster().addAndStartNode(provisioner, flags);
    }
}
