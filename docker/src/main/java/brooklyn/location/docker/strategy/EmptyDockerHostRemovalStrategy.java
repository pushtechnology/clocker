package brooklyn.location.docker.strategy;


import java.util.Collection;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.entity.trait.Startable;

import brooklyn.entity.container.docker.DockerHost;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * Only remove hosts that do not have any containers.
 * @author Matt Champion on 02/05/15
 */
public final class EmptyDockerHostRemovalStrategy implements Function<Collection<Entity>, Entity> {
    private static final Predicate<Entity> EMPTY_HOST_PREDICATE = new EmptyHostPredicate();

    public EmptyDockerHostRemovalStrategy() {
    }

    @Override
    public Entity apply(Collection<Entity> entities) {
        if (entities == null) {
            return null;
        }

        // Filter hosts with running containers
        final Iterable<Entity> emptyHosts = Iterables.filter(entities, EMPTY_HOST_PREDICATE);

        // choose newest entity that is stoppable, or if none are stoppable take the newest non-stoppable
        long newestTime = 0;
        Entity newest = null;

        for (Entity contender : emptyHosts) {
            boolean newer = contender.getCreationTime() > newestTime;
            if ((contender instanceof Startable && newer) ||
                (!(newest instanceof Startable) && ((contender instanceof Startable) || newer))) {
                newest = contender;
                newestTime = contender.getCreationTime();
            }
        }
        return newest;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    @Override
    public int hashCode() {
        return 7;
    }

    @Override
    public boolean equals(Object object) {
        return object != null && (object == this || object instanceof EmptyDockerHostRemovalStrategy);
    }

    private static final class EmptyHostPredicate implements Predicate<Entity> {
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
}
