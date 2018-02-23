package org.broadinstitute.hellbender.tools.walkers.realignmentfilter;


import htsjdk.samtools.SAMFileHeader;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAligner;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAlignment;
import org.broadinstitute.hellbender.utils.bwa.BwaMemIndex;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public class Realigner {
    private final BwaMemAligner aligner;
    private final int minMappingQuality;

    public Realigner(final RealignmentArgumentCollection rfac, final int minMappingQuality) {
        final BwaMemIndex index = new BwaMemIndex(rfac.bwaMemIndexImage);
        this.minMappingQuality = minMappingQuality;
        aligner = new BwaMemAligner(index);
        aligner.setMinSeedLengthOption(rfac.minSeedLength);
        aligner.setDropRatioOption((float) rfac.dropRatio);
        aligner.setSplitFactorOption((float) rfac.splitFactor);
        if (!rfac.dontUseMates) {
            aligner.alignPairs();
        }
    }

    public RealignmentResult realign(final GATKRead read) {
        final List<BwaMemAlignment> alignments = aligner.alignSeqs(Arrays.asList(read), GATKRead::getBasesNoCopy).get(0);
        return checkAlignments(alignments);
    }

    public RealignmentResult realign(final GATKRead read, final GATKRead mate) {
        final List<BwaMemAlignment> alignments = aligner.alignSeqs(Arrays.asList(read, mate), GATKRead::getBasesNoCopy).get(0);
        return checkAlignments(alignments);
    }

    public RealignmentResult checkAlignments(final List<BwaMemAlignment> alignments) {
        if (alignments.isEmpty()) {
            return new RealignmentResult(true, Collections.emptyList());
        }

        if (alignments.size() > 1) {
            // sort by descending mapping quality
            Collections.sort(alignments, Comparator.comparingInt(BwaMemAlignment::getMapQual).reversed());
        }

        final BwaMemAlignment alignment = alignments.get(0);
        if (alignment.getMapQual() < minMappingQuality) {
            return new RealignmentResult(false, alignments);
        }

        // TODO: perhaps check number of mismatches in second best alignment?

        final int contigId = alignment.getRefId();
        if (contigId < 0) {
            return new RealignmentResult(true, alignments);
        }

        // TODO: we need to check that contig is the same or equivalent up to hg38 alt contig
        // TODO: do this by looking at index.getReferenceContigNames() and bamHeader.getSequenceDictionary().getSequences()
        // TODO: in IDE and seeing what the correspondence could be
        // TODO: put in check that start position is within eg 10 Mb of original mapping

        return new RealignmentResult(true, alignments);
    }

    public static class RealignmentResult {
        private final boolean mapsToSupposedLocation;
        private final List<BwaMemAlignment> realignments;

        public RealignmentResult(boolean mapsToSupposedLocation, List<BwaMemAlignment> realignments) {
            this.mapsToSupposedLocation = mapsToSupposedLocation;
            this.realignments = realignments;
        }

        public boolean isGood() { return mapsToSupposedLocation;  }

        public List<BwaMemAlignment> getRealignments() { return realignments; }
    }
}