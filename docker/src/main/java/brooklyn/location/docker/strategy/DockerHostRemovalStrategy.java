package brooklyn.location.docker.strategy;

import java.util.Collection;

import javax.annotation.Nullable;

import brooklyn.entity.container.docker.DockerHost;
import brooklyn.entity.trait.Startable;
import com.google.common.base.Function;
import brooklyn.entity.Entity;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * Only remove hosts that do not have any containers.
 * @author Matt Champion on 02/05/15
 */
public final class DockerHostRemovalStrategy implements Function<Collection<Entity>, Entity> {
    private static final Predicate<Entity> EMPTY_HOST_PREDICATE = new EmptyHostPredicate();

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

    private static final class EmptyHostPredicate implements Predicate<Entity> {
        @Override
        public boolean apply(Entity entity) {
            if (entity == null) {
                return false;
            }
            else if (entity instanceof DockerHost) {
                DockerHost host = (DockerHost)entity;
                if (host.getDockerContainerList().isEmpty()) {
                    return true;
                }
                else {
                    return false;
                }
            }
            else {
                return false;
            }
        }
    }
}
