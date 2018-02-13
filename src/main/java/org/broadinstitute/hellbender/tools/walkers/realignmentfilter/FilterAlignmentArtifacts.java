package org.broadinstitute.hellbender.tools.walkers.realignmentfilter;

import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.ReadFilterArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadsContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.engine.VariantWalker;
import org.broadinstitute.hellbender.tools.exome.FilterByOrientationBias;
import org.broadinstitute.hellbender.tools.walkers.contamination.CalculateContamination;
import org.broadinstitute.hellbender.tools.walkers.mutect.*;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.read.AlignmentUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;
import picard.cmdline.programgroups.VariantFilteringProgramGroup;

import java.io.File;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>Filter false positive alignment artifacts from a VCF callset.</p>
 *
 * <p>
 *     FilterAlignmentArtifacts identifies alignment artifacts, that is, apparent variants due to reads being mapped to the wrong genomic locus.
 * </p>
 * <p>
 *     Alignment artifacts can occur whenever there is sufficient sequence similarity between two or more regions in the genome
 *     to confuse the alignment algorithm.  This can occur when the aligner for whatever reason overestimate how uniquely a read
 *     maps, thereby assigning it too high of a mapping quality.  It can also occur through no fault of the aligner due to gaps in
 *     the reference, which can also hide the true position to which a read should map.  By using a good alignment algorithm
 *     (the GATK wrapper of BWA-MEM), giving it sensitive settings (which may have been impractically slow for the original
 *     bam alignment) and mapping to the best available reference we can avoid these pitfalls.  The last point is especially important:
 *     one can (and should) use a BWA-MEM index image corresponding to the best reference, regardless of the reference to which
 *     the bam was aligned.
 * </p>
 * <p>
 *     This tool is featured in the Somatic Short Mutation calling Best Practice Workflow.
 *     See <a href="https://software.broadinstitute.org/gatk/documentation/article?id=11136">Tutorial#11136</a> for a
 *     step-by-step description of the workflow and <a href="https://software.broadinstitute.org/gatk/documentation/article?id=11127">Article#11127</a>
 *     for an overview of what traditional somatic calling entails. For the latest pipeline scripts, see the
 *     <a href="https://github.com/broadinstitute/gatk/tree/master/scripts/mutect2_wdl">Mutect2 WDL scripts directory</a>.
 * </p>
 * <p>
 *     The bam input to this tool should be the reassembly bamout produced by HaplotypeCaller or Mutect2 in the process of generating
 *     the input callset.  The original bam will also work but might fail to filter some indels.  The reference passed with the -R argument
 *     but be the reference to which the input bam was realigned.  This does not need to correspond to the reference of the BWAS-MEM
 *     index image.  The latter should be derived from the best available reference, for example hg38 in humans as of February 2018.
 * </p>
 *
 * <h3>Usage example</h3>
 * <pre>
 * gatk FilterAlignmentArtifacts \
 *   -R hg19.fasta
 *   -V somatic.vcf.gz \
 *   -I somatic_bamout.bam \
 *   --bwa-mem-index-image hg38.index_image \
 *   -O filtered.vcf.gz
 * </pre>
 *
 */
@CommandLineProgramProperties(
        summary = "Filter alignment artifacts from a vcf callset.",
        oneLineSummary = "Filter alignment artifacts from a vcf callset.",
        programGroup = VariantFilteringProgramGroup.class
)
@DocumentedFeature
@BetaFeature
public class FilterAlignmentArtifacts extends VariantWalker {

    @Argument(fullName= StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            shortName=StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            doc="The output filtered VCF file", optional=false)
    private final String outputVcf = null;

    @ArgumentCollection
    protected RealignmentArgumentCollection realignmentArgumentCollection = new RealignmentArgumentCollection();

    private SAMFileHeader header;
    private VariantContextWriter vcfWriter;
    private Realigner realigner;

    @Override
    public boolean requiresReads() { return true; }

    @Override
    public boolean requiresReference() { return true; }

    @Override
    public void onTraversalStart() {
        header = getHeaderForReads();
        realigner = new Realigner(realignmentArgumentCollection, getHeaderForReads());
        vcfWriter = createVCFWriter(new File(outputVcf));

        final VCFHeader inputHeader = getHeaderForVariants();
        final Set<VCFHeaderLine> headerLines = inputHeader.getMetaDataInSortedOrder();
        headerLines.add(GATKVCFHeaderLines.getFilterLine(GATKVCFConstants.ALIGNMENT_ARTIFACT_FILTER_NAME));
        headerLines.addAll(getDefaultToolVCFHeaderLines());
        final VCFHeader vcfHeader = new VCFHeader(headerLines, inputHeader.getGenotypeSamples());
        vcfWriter.writeHeader(vcfHeader);

    }

    @Override
    public Object onTraversalSuccess() {
        return "SUCCESS";
    }

    @Override
    public void apply(final VariantContext vc, final ReadsContext readsContext, final ReferenceContext refContext, final FeatureContext fc) {
        if (vc.getNAlleles() == 1) {
            vcfWriter.add(vc);
            return;
        }
readsContext.getInterval()
        final List<String> variantSamples = vc.getGenotypes().stream().filter(g -> !g.isHomRef()).map(g -> g.getSampleName()).collect(Collectors.toList());

        final int readLength = Utils.stream(readsContext).mapToInt(GATKRead::getLength).max().getAsInt();

        refContext.getBases()

        final MutableInt failedRealignmentCount = new MutableInt(0);
        final MutableInt succeededRealignmentCount = new MutableInt(0);
        for (final GATKRead read : readsContext) {
            if (!variantSamples.contains(ReadUtils.getSampleName(read, header))) {
                continue;
            }
            if (readIsConsistentWithVariant(read, vc)) {

            }
        }

        final VariantContextBuilder vcb = new VariantContextBuilder(vc);
        vcfWriter.add(vcb.make());
    }

    @Override
    public void closeTool() {
        if ( vcfWriter != null ) {
            vcfWriter.close();
        }
    }

    private static boolean readIsConsistentWithVariant(final GATKRead read, final VariantContext vc) {
        final byte[] readBases = read.getBasesNoCopy();
        final int readIndexOfVariant = ReadUtils.getReadCoordinateForReferenceCoordinate(read.getSoftStart(), read.getCigar(),
                vc.getStart(), ReadUtils.ClippingTail.RIGHT_TAIL, true);
        if ( readIndexOfVariant == ReadUtils.CLIPPING_GOAL_NOT_REACHED || AlignmentUtils.isInsideDeletion(read.getCigar(), readIndexOfVariant)) {
            return false;
        }

        final int referenceLength = vc.getReference().length();
        for (final Allele allele : vc.getAlternateAlleles()) {
            if (allele.length() == referenceLength) {   // SNP or MNP -- check whether read bases match variant allele bases
                if (allele.basesMatch(ArrayUtils.subarray(readBases, readIndexOfVariant, Math.min(readIndexOfVariant + allele.length(), readBases.length)))) {
                    return true;
                }
            } else {    // indel -- check if the read has the right CIGAR operator at this position
                final boolean isDeletion = allele.length() < referenceLength;
                int baseBeforeCigarElement = -1;    // offset by 1 because the variant position is one base before the first indel base
                for (final CigarElement cigarElement : read.getCigarElements()) {
                    if (baseBeforeCigarElement == readIndexOfVariant) {
                        if (isDeletion && cigarElement.getOperator() == CigarOperator.D || !isDeletion && cigarElement.getOperator() == CigarOperator.I) {
                            return true;
                        }
                    } else {
                        baseBeforeCigarElement += cigarElement.getLength();
                    }
                }
            }
        }
        return false;
    }
}
