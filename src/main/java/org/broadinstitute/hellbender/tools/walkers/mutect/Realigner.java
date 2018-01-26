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

    public boolean mapsToSupposedLocation(final GATKRead read) {
        read.
        final String assignedContig = read.getAssignedContig();

        if (assignedContig == null) {
            return false;
        }

        final List<BwaMemAlignment> alignments = aligner.alignSeqs(Arrays.asList(read), GATKRead::getBases).get(0);

        if (alignments.isEmpty()) {
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

        // TODO: perhaps check number of mismatches in second best alignment?

        final int contigId = alignment.getRefId();
        if (contigId < 0) {
            return false;
        }

        // TODO: we need to check that contig is the same or equivalent up to hg38 alt contig
        // TODO: do this by looking at index.getReferenceContigNames() and bamHeader.getSequenceDictionary().getSequences()
        // TODO: in IDE and seeing what the correspondence could be
        // TODO: put in check that start position is within eg 10 Mb of original mapping

        return true;
    }
}