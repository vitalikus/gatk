package org.broadinstitute.hellbender.tools.spark.sv.evidence;

import biz.k11i.xgboost.Predictor;
import biz.k11i.xgboost.learner.ObjFunction;
import com.google.common.annotations.VisibleForTesting;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryArgumentCollection;
import org.broadinstitute.hellbender.tools.spark.sv.utils.*;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A class that acts as a filter for breakpoint evidence.
 * It passes only that evidence that is part of a putative cluster.
 */
public final class XGBoostEvidenceFilter implements Iterator<BreakpointEvidence> {
    static private final boolean useFastMathExp = true;
    private final ReadMetadata readMetadata;
    private final PartitionCrossingChecker partitionCrossingChecker;

    private final Predictor predictor;
    private final double coverage;
    private final int minEvidenceMapQ;
    private final double thresholdProbability;

    private final SVIntervalTree<List<BreakpointEvidence>> evidenceTree;

    private Iterator<SVIntervalTree.Entry<List<BreakpointEvidence>>> treeItr;
    private Iterator<BreakpointEvidence> listItr;

    public XGBoostEvidenceFilter(final Iterator<BreakpointEvidence> evidenceItr,
                                 final ReadMetadata readMetadata,
                                 final StructuralVariationDiscoveryArgumentCollection.FindBreakpointEvidenceSparkArgumentCollection params,
                                 final PartitionCrossingChecker partitionCrossingChecker) {
        this.predictor = loadPredictor(params.svEvidenceFilterModelFile);
        this.readMetadata = readMetadata;
        this.partitionCrossingChecker = partitionCrossingChecker;
        this.coverage = readMetadata.getCoverage();
        this.minEvidenceMapQ = params.minEvidenceMapQ;
        this.thresholdProbability = params.svEvidenceFilterThresholdProbability;


        this.evidenceTree = buildTree(evidenceItr);
        this.treeItr = evidenceTree.iterator();
        this.listItr = null;
    }

    public static Predictor loadPredictor(final String modelFileLocation) {
        ObjFunction.useFastMathExp(useFastMathExp);
        try {
            final InputStream modelStream = modelFileLocation.startsWith("/large/") ?
                    XGBoostEvidenceFilter.class.getResourceAsStream(modelFileLocation)
                    : BucketUtils.openFile(modelFileLocation);
            //return new Predictor(new FileInputStream(classifierModelFile));
            return new Predictor(modelStream);
        } catch(IOException e) {
            throw new GATKException(
                    "Unable to load predictor from classifier file " + modelFileLocation + ": " + e.getMessage()
            );
        }
    }

    @Override
    public boolean hasNext() {
        if ( listItr != null && listItr.hasNext() ) {
            return true;
        }
        listItr = null;
        boolean result = false;
        while ( !result && treeItr.hasNext() ) {
            final SVIntervalTree.Entry<List<BreakpointEvidence>> entry = treeItr.next();
            final SVInterval curInterval = entry.getInterval();
            final List<BreakpointEvidence> evidenceList = entry.getValue();
            if( isValidated(entry.getValue()) || partitionCrossingChecker.onBoundary(curInterval) ) {
                // already validated (no need to mark validated again) or on partition boundary (punt for now)
                result = true;
            } else if( anyPassesFilter(evidenceList) ) {
                evidenceList.forEach(ev -> ev.setValidated(true));
                result = true;
            }

            if ( result ) {
                listItr = entry.getValue().iterator();
            }
        }
        return result;
    }

    @Override
    public BreakpointEvidence next() {
        if ( !hasNext() ) {
            throw new NoSuchElementException("No next element.");
        }
        return listItr.next();
    }

    private static SVIntervalTree<List<BreakpointEvidence>> buildTree( final Iterator<BreakpointEvidence> evidenceItr ) {
        SVIntervalTree<List<BreakpointEvidence>> tree = new SVIntervalTree<>();
        while ( evidenceItr.hasNext() ) {
            final BreakpointEvidence evidence = evidenceItr.next();
            addToTree(tree, evidence.getLocation(), evidence);
        }
        return tree;
    }

    private boolean isValidated( final List<BreakpointEvidence> evList ) {
        for ( final BreakpointEvidence ev : evList ) {
            if ( ev.isValidated() ) return true;
        }
        return false;
    }

    @VisibleForTesting
    boolean anyPassesFilter(final List<BreakpointEvidence> evidenceList) {
        for(final BreakpointEvidence evidence : evidenceList) {
            final double evidenceGoodProbability = predictor.predictSingle(getFeatures(evidence));
            if(evidenceGoodProbability > thresholdProbability) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    EvidenceFeatures getFeatures(final BreakpointEvidence evidence) {
        // create new struct for these two, use CigarOperator to update if instanceof ReadEvidence
        final double bases_matched = 0.0;
        final double ref_length = 0.0;
        // need to find map from evidence_type to integer codes!!!
        final double evidence_type = 0.0;
        final double mappingQuality = getMappingQuality(evidence);
        // should be similar strategegy to getMappingQuality, except store read_count if not ReadEvidence, and assert
        // it's a TemplateSizeAnomaly
        final double template_size = 0.0;
        // calculate these similar to BreakpointDensityFilter, but always calculate full totals, never end early.
        final double num_overlap = 0.0;
        final double num_coherent = 0.0;

        
        return new EvidenceFeatures(new double[]{bases_matched, ref_length, evidence_type, mappingQuality,
                                                 template_size, num_overlap, num_coherent});
    }

    private double getMappingQuality(final BreakpointEvidence evidence) {
        return evidence instanceof BreakpointEvidence.ReadEvidence ?
            ((BreakpointEvidence.ReadEvidence) evidence).getMappingQuality() : 0.0;
    }





    @VisibleForTesting boolean hasEnoughOverlappers( final SVInterval interval ) {
        final Iterator<SVIntervalTree.Entry<List<BreakpointEvidence>>> itr = evidenceTree.overlappers(interval);
        PairedStrandedIntervalTree<BreakpointEvidence> targetIntervalTree = new PairedStrandedIntervalTree<>();
        int weight = 0;
        while ( itr.hasNext() ) {
            final List<BreakpointEvidence> evidenceForInterval = itr.next().getValue();
            weight += evidenceForInterval.stream().mapToInt(BreakpointEvidence::getWeight).sum();
            if ( weight >= minEvidenceWeight) {
                return true;
            }

            for (final BreakpointEvidence evidence : evidenceForInterval) {
                if (evidence.hasDistalTargets(readMetadata, minEvidenceMapQ)) {
                    final List<StrandedInterval> distalTargets = evidence.getDistalTargets(readMetadata, minEvidenceMapQ);
                    for (int i = 0; i < distalTargets.size(); i++) {
                        targetIntervalTree.put(
                                new PairedStrandedIntervals(
                                        new StrandedInterval(evidence.getLocation(), evidence.isEvidenceUpstreamOfBreakpoint()),
                                        distalTargets.get(i)),
                                evidence
                                );
                    }
                }
            }
        }

        final Iterator<Tuple2<PairedStrandedIntervals, BreakpointEvidence>> targetLinkIterator = targetIntervalTree.iterator();
        while (targetLinkIterator.hasNext()) {
            Tuple2<PairedStrandedIntervals, BreakpointEvidence> next = targetLinkIterator.next();
            final int coherentEvidenceWeight = (int) Utils.stream(targetIntervalTree.overlappers(next._1())).count();
            if (coherentEvidenceWeight >= minCoherentEvidenceWeight) {
                return true;
            }

        }

        return false;
    }

    private static <T> void addToTree( final SVIntervalTree<List<T>> tree,
                                   final SVInterval interval,
                                   final T value ) {
        final SVIntervalTree.Entry<List<T>> entry = tree.find(interval);
        if ( entry != null ) {
            entry.getValue().add(value);
        } else {
            final List<T> valueList = new ArrayList<>(1);
            valueList.add(value);
            tree.put(interval, valueList);
        }
    }
}
