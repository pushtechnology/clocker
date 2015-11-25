package brooklyn.location.docker.strategy;

import org.apache.brooklyn.api.entity.Entity;

import brooklyn.entity.container.docker.DockerHost;
import com.google.common.base.Predicate;

/**
 * A predicate that is {@code true} only for empty docker hosts.
 * @author Matt Champion on 02/05/15
 */
public final class EmptyHostPredicate implements Predicate<Entity> {
    @Override
    public boolean apply(Entity entity) {
        if (entity != null && entity instanceof DockerHost) {
            final DockerHost host = (DockerHost) entity;
            return host.getDockerContainerList().isEmpty();
        }
        else {
            return false;
        }
    }
}
