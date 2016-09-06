package clocker.docker.location.strategy;

import static com.google.api.client.util.Maps.newHashMap;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Math.min;
import static java.util.Collections.sort;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.entity.group.DynamicCluster.NodePlacementStrategy;
import org.apache.brooklyn.entity.group.zoneaware.BalancingNodePlacementStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

/**
 * Implementation of {@link NodePlacementStrategy} based on {@link BalancingNodePlacementStrategy} but does not remove
 * docker hosts with containers.
 * @author Matt Champion on 24/11/2015
 */
public final class DockerHostNodePlacementStrategy implements NodePlacementStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(DockerHostNodePlacementStrategy.class);

    @Override
    public List<Location> locationsForAdditions(
            Multimap<Location, Entity> currentMembers, Collection<? extends Location> locs, int numToAdd) {
        if (locs.isEmpty() && numToAdd > 0) {
            throw new IllegalArgumentException("No locations supplied, when requesting locations for " + numToAdd + " nodes");
        }

        final List<Location> result = newArrayList();
        final Map<Location, Integer> locSizes = toMutableLocationSizes(currentMembers, locs);

        for (int i = 0; i < numToAdd; i++) {
            // TODO Inefficient to loop this many times! But not called with big numbers.
            Location leastPopulatedLoc = null;
            int leastPopulatedLocSize = 0;
            for (Location loc : locs) {
                int locSize = locSizes.get(loc);
                if (leastPopulatedLoc == null || locSize < leastPopulatedLocSize) {
                    leastPopulatedLoc = loc;
                    leastPopulatedLocSize = locSize;
                }
            }

            assert leastPopulatedLoc != null : "leastPopulatedLoc=null; locs=" + locs + "; currentMembers=" + currentMembers;
            result.add(leastPopulatedLoc);
            locSizes.put(leastPopulatedLoc, locSizes.get(leastPopulatedLoc) + 1);
        }
        return result;
    }

    @Override
    public List<Entity> entitiesToRemove(Multimap<Location, Entity> currentMembers, int numToRemove) {
        if (currentMembers.isEmpty()) {
            throw new IllegalArgumentException(
                "No members supplied, when requesting removal of " + numToRemove + " nodes");
        }

        if (currentMembers.size() < numToRemove) {
            LOG.warn(
                "Request to remove " +
                    numToRemove +
                    " when only " +
                    currentMembers.size() +
                    " members (continuing): " +
                    currentMembers);
            numToRemove = currentMembers.size();
        }

        final Map<Location, Integer> numToRemovePerLoc = newHashMap();
        final Map<Location, Integer> locSizes = toMutableLocationSizes(currentMembers, ImmutableList.<Location>of());

        for (int i = 0; i < numToRemove; i++) {
            // TODO Inefficient to loop this many times! But not called with big numbers.
            Location mostPopulatedLoc = null;
            int mostPopulatedLocSize = 0;
            for (Location loc : locSizes.keySet()) {
                int locSize = locSizes.get(loc);
                if (locSize > 0 && (mostPopulatedLoc == null || locSize > mostPopulatedLocSize)) {
                    mostPopulatedLoc = loc;
                    mostPopulatedLocSize = locSize;
                }
            }

            assert mostPopulatedLoc != null : "leastPopulatedLoc=null; currentMembers=" + currentMembers;
            numToRemovePerLoc.put(
                mostPopulatedLoc,
                ((numToRemovePerLoc.get(mostPopulatedLoc) == null) ? 0 : numToRemovePerLoc.get(mostPopulatedLoc)) + 1);
            locSizes.put(mostPopulatedLoc, locSizes.get(mostPopulatedLoc) - 1);
        }

        final List<Entity> result = newArrayList();
        for (Map.Entry<Location, Integer> entry : numToRemovePerLoc.entrySet()) {
            result.addAll(pickNewestEmptyDockerHost(currentMembers.get(entry.getKey()), entry.getValue()));
        }
        return result;
    }

    protected Map<Location,Integer> toMutableLocationSizes(
            Multimap<Location, Entity> currentMembers, Iterable<? extends Location> otherLocs) {
        final Map<Location,Integer> result = newHashMap();

        // Record the number of values for each key in the multimap
        for (Location key : currentMembers.keySet()) {
            result.put(key, currentMembers.get(key).size());
        }

        // Record 0 for locations not in the multimap
        for (Location otherLoc : otherLocs) {
            if (!result.containsKey(otherLoc)) {
                result.put(otherLoc, 0);
            }
        }
        return result;
    }

    protected Collection<Entity> pickNewestEmptyDockerHost(Collection<Entity> contenders, Integer numToPick) {
        // choose newest empty docker host that is stoppable; sort so newest is first
        final List<Entity> stoppables = newArrayList(
            filter(
                filter(
                    contenders,
                    instanceOf(Startable.class)),
                new EmptyHostPredicate()));

        sort(stoppables, new Comparator<Entity>() {
            @Override
            public int compare(Entity a, Entity b) {
                return (int) (b.getCreationTime() - a.getCreationTime());
            }
        });

        return stoppables.subList(0, min(numToPick, stoppables.size()));
    }
}
