package org.broadinstitute.hellbender.tools.copynumber.utils.annotatedregion;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.tribble.AsciiFeatureCodec;
import htsjdk.tribble.readers.LineIterator;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.codecs.xsvLocatableTable.XsvLocatableTableCodec;
import org.broadinstitute.hellbender.utils.codecs.xsvLocatableTable.XsvTableFeature;
import org.broadinstitute.hellbender.utils.io.Resource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Read AnnotatedIntervals from a xsv file (see {@link XsvLocatableTableCodec}.
 *
 */
public class AnnotatedIntervalCodec extends AsciiFeatureCodec<AnnotatedInterval> {

    private XsvLocatableTableCodec xsvLocatableTableCodec;
    private AnnotatedIntervalHeader header;

    public AnnotatedIntervalCodec() {
        super(AnnotatedInterval.class);
        xsvLocatableTableCodec = new XsvLocatableTableCodec();
    }

    public AnnotatedIntervalCodec(final Path overrideConfigFile) {
        super(AnnotatedInterval.class);
        xsvLocatableTableCodec = new XsvLocatableTableCodec(overrideConfigFile);
    }

    @Override
    public AnnotatedInterval decode(final String s) {
        final XsvTableFeature feature = xsvLocatableTableCodec.decode(s);
        if (feature == null) {
            return null;
        }

        final List<String> annotationFields = header.getAnnotations();
        final SortedMap<String, String> annotations = new TreeMap<>();
        IntStream.range(0, annotationFields.size()).boxed()
                .forEach(i -> annotations.put(annotationFields.get(i), feature.getValuesWithoutLocationColumns().get(i)));

        return new AnnotatedInterval(
                new SimpleInterval(feature.getContig(), feature.getStart(), feature.getEnd()),
                annotations);
    }

    @Override
    public AnnotatedIntervalHeader readActualHeader(final LineIterator reader) {
        xsvLocatableTableCodec.readActualHeader(reader);
        header = new AnnotatedIntervalHeader(xsvLocatableTableCodec.getContigColumn(), xsvLocatableTableCodec.getStartColumn(),
                xsvLocatableTableCodec.getEndColumn(), xsvLocatableTableCodec.getHeaderWithoutLocationColumns(),
                xsvLocatableTableCodec.renderSamFileHeader());
        return header;
    }

    @Override
    public boolean canDecode(final String path) {
        return xsvLocatableTableCodec.canDecode(path);
    }

    /**
     * Create an annotated interval header based on a config file (for locatable field names only) and a list of annotations (the rest of the fields).
     *
     * @param outputConfigFile config path for determining the locatable column headers.  Never {@code null}.
     * @param annotations  Names of the annotations to render.  If any of the locatable columns are in the annotation, those columns will be removed from the annotations list in the header.
     *                     Never {@code null}.
     * @param samFileHeader SAM FileHeader to prepend to the data.  {@code null} is allowed.
     * @return a header that can be used in an AnnotatedFileWriter.  Never {@code null}.
     */
    public static AnnotatedIntervalHeader createHeaderForWriter(final Path outputConfigFile, final List<String> annotations, final SAMFileHeader samFileHeader) {

        Utils.nonNull(annotations);
        Utils.nonNull(outputConfigFile);

        final Properties headerNameProperties = XsvLocatableTableCodec.getAndValidateConfigFileContents(outputConfigFile);
        final String contigColumnName = headerNameProperties.getProperty(XsvLocatableTableCodec.CONFIG_FILE_CONTIG_COLUMN_KEY);
        final String startColumnName = headerNameProperties.getProperty(XsvLocatableTableCodec.CONFIG_FILE_START_COLUMN_KEY);
        final String endColumnName = headerNameProperties.getProperty(XsvLocatableTableCodec.CONFIG_FILE_END_COLUMN_KEY);

        XsvLocatableTableCodec.validateLocatableColumnName(contigColumnName);
        XsvLocatableTableCodec.validateLocatableColumnName(startColumnName);
        XsvLocatableTableCodec.validateLocatableColumnName(endColumnName);

        final List<String> finalAnnotations = annotations.stream()
                .filter(a -> !a.equals(contigColumnName) && !a.equals(startColumnName) && !a.equals(endColumnName))
                .collect(Collectors.toList());

        return new AnnotatedIntervalHeader(contigColumnName, startColumnName, endColumnName, finalAnnotations, samFileHeader);
    }

    /**
     *  See {@link #createHeaderForWriter(Path, List, SAMFileHeader)}
     *
     *  This will use the default headers for annotated regions.  Call this method if no config file is available.
     *
     * @param annotations Annotations that should be used in the header.  Never {@code null}.
     * @param samFileHeader SAM File header to use for this header.  {@code null} is allowed.
     * @return A header to be used for output.  Never {@code null}
     */
    public static AnnotatedIntervalHeader createHeaderForWriter(final List<String> annotations, final SAMFileHeader samFileHeader) {
        Utils.nonNull(annotations);

        try {
            final Path resourceFile = Resource.getResourceContentsAsFile(AnnotatedIntervalCollection.ANNOTATED_INTERVAL_DEFAULT_CONFIG_RESOURCE).toPath();
            return createHeaderForWriter(resourceFile, annotations, samFileHeader);
        } catch (final IOException ioe) {
            throw new GATKException.ShouldNeverReachHereException("Could not load the default config file for annotated intervals.", ioe);
        }
    }
}
