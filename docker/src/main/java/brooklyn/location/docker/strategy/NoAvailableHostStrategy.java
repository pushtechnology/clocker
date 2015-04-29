package brooklyn.location.docker.strategy;

import java.util.Map;

import brooklyn.entity.Entity;
import brooklyn.entity.container.docker.DockerHost;
import brooklyn.location.NoMachinesAvailableException;
import brooklyn.location.docker.DockerLocation;
import brooklyn.policy.Policy;

/**
 * Action to take when attempting to provision an entity in a docker cloud location and no hosts are available.
 * @author Matt Champion on 24/04/15
 */
public interface NoAvailableHostStrategy extends Policy {

    /**
     * Handle the behaviour when their are no hosts
     * @param location The location that has no hosts
     * @param newEntity The entity being deployed
     * @param flags The flags to provision with
     * @return A newly provisioned host
     * @throws NoMachinesAvailableException if a host cannot be provisioned
     */
    DockerHost handleNoHosts(DockerLocation location, Entity newEntity, Map<?, ?> flags) throws NoMachinesAvailableException;
}
