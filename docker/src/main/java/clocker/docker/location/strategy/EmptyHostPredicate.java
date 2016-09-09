package clocker.docker.location.strategy;

import org.apache.brooklyn.api.entity.Entity;

import com.google.common.base.Predicate;

import clocker.docker.entity.DockerHost;

/**
 * A predicate that is {@code true} only for empty docker hosts.
 * @author Matt Champion on 02/05/15
 */
public final class EmptyHostPredicate implements Predicate<Entity> {
    @Override
    public boolean apply(Entity entity) {
        if (entity != null && entity instanceof DockerHost) {
            DockerHost host = (DockerHost)entity;
            return host.getDockerContainerList().isEmpty();
        }
        else {
            return false;
        }
    }
}
