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
