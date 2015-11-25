package brooklyn.location.docker.strategy;

import static com.google.api.client.util.Maps.newHashMap;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Math.min;
import static java.util.Collections.max;
import static java.util.Collections.sort;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

        LOG.debug("Available locations {}", result);

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
        LOG.debug("Current hosts by location {}", locSizes);

        for (int i = 0; i < numToRemove; i++) {
            // Find the location with the most entities currently in
            final Entry<Location, Integer> mostPopulated = max(
                locSizes.entrySet(),
                new Comparator<Entry<Location, Integer>>() {
                    @Override
                    public int compare(Entry<Location, Integer> entry0, Entry<Location, Integer> entry1) {
                        return entry0.getValue().compareTo(entry1.getValue());
                    }
                });

            assert mostPopulated != null : "mostPopulated=null; currentMembers=" + currentMembers;
            final Location mostPopulatedLocation = mostPopulated.getKey();
            final Integer mostPopulatedSize = mostPopulated.getValue();

            // Update the number to remove per location
            final Integer currentNumberToRemove = numToRemovePerLoc.get(mostPopulatedLocation);
            if (currentNumberToRemove == null) {
                numToRemovePerLoc.put(mostPopulatedLocation, 1);
            }
            else {
                numToRemovePerLoc.put(mostPopulatedLocation, currentNumberToRemove + 1);
            }

            // Update the target number per location
            locSizes.put(mostPopulatedLocation, mostPopulatedSize - 1);
        }

        // Find the entities to remove per location
        final List<Entity> result = newArrayList();
        for (Entry<Location, Integer> entry : numToRemovePerLoc.entrySet()) {
            final Location location = entry.getKey();
            final Integer numberToRemove = entry.getValue();
            LOG.error("Looking for {} entities to remove from {}", numberToRemove, location);
            result.addAll(pickNewestEmptyDockerHost(currentMembers.get(location), numberToRemove));
        }

        return result;
    }

    private Map<Location,Integer> toMutableLocationSizes(
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

    private Collection<Entity> pickNewestEmptyDockerHost(Collection<Entity> contenders, int numToPick) {
        LOG.debug("Selecting from {}", contenders);

        // choose newest empty docker host that is stoppable; sort so newest is first
        final List<Entity> candidates = newArrayList(
            filter(
                filter(
                    contenders,
                    instanceOf(Startable.class)),
                new EmptyHostPredicate()));

        LOG.debug("Candidates for removal {}", candidates);

        sort(candidates, new Comparator<Entity>() {
            @Override
            public int compare(Entity a, Entity b) {
                return (int) (b.getCreationTime() - a.getCreationTime());
            }
        });

        final List<Entity> selected = candidates.subList(0, min(numToPick, candidates.size()));
        LOG.debug("Selected for removal {}", selected);
        return selected;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
