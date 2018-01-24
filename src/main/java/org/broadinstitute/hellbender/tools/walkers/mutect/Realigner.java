package org.broadinstitute.hellbender.tools.walkers.mutect;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.liftover.LiftOver;
import htsjdk.samtools.util.Interval;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAligner;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAlignment;
import org.broadinstitute.hellbender.utils.bwa.BwaMemIndex;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.*;
import java.util.function.Function;

public class Realigner {
    public static final int MIN_MAP_QUALITY_FOR_REALIGNED_READS = 40;
    private final BwaMemAligner aligner;
    private final List<String> realignmentContigs;

    public Realigner(final RealignmentFilterArgumentCollection rfac, final SAMFileHeader bamHeader) {
        final BwaMemIndex index = new BwaMemIndex(rfac.bwaMemIndexImage);
        realignmentContigs = index.getReferenceContigNames();
        aligner = new BwaMemAligner(index);
        aligner.setMinSeedLengthOption(rfac.minSeedLength);
        aligner.setDropRatioOption((float) rfac.dropRatio);
        aligner.setSplitFactorOption((float) rfac.splitFactor);
    }

    public boolean mapsToSupposedLocation(final GATKRead read, final Interval supposedRealignmentLocation) {

        final List<BwaMemAlignment> alignments = aligner.alignSeqs(Arrays.asList(read), GATKRead::getBases).get(0);
        if (supposedRealignmentLocation == null) {
            return false;
        }
        //TODO Incomplete!!!!!
        if (alignments.isEmpty()) { // does this ever occur?
            return false;
        }

        if (alignments.size() > 1) {
            // sort by descending mapping quality
            Collections.sort(alignments, Comparator.comparingInt(BwaMemAlignment::getMapQual).reversed());
        }

        final BwaMemAlignment alignment = alignments.get(0);
        if (alignment.getMapQual() < MIN_MAP_QUALITY_FOR_REALIGNED_READS) {
            return false;
        }

        final int contigId = alignment.getRefId();
        if (contigId < 0) {
            return false;
        }

        return new Interval(realignmentContigs.get(contigId), alignment.getRefStart(), alignment.getRefEnd()).overlaps(supposedRealignmentLocation);
    }
}