package org.broadinstitute.hellbender.tools.spark.sv.evidence;

import biz.k11i.xgboost.Predictor;
import biz.k11i.xgboost.learner.ObjFunction;
import biz.k11i.xgboost.util.FVec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.GATKBaseTest;
import org.broadinstitute.hellbender.engine.spark.SparkContextFactory;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class XGBoostEvidenceFilterUnitTest extends GATKBaseTest {
    private static final String testDataJsonFile = publicTestDir + "sv_classifier_test_data.json";
    private static final String classifierModelFile = "/large/sv_evidence_classifier.bin";
    private static final String resourceClassifierModelFile = "gatk-resources::" + classifierModelFile;
    private static final String localClassifierModelFile
            = new File(publicMainResourcesDir, classifierModelFile).getAbsolutePath();
    private static final String gcsClassifierModelFile
            = "gs://broad-dsde-methods/sv/reference/GRCh38/sv_evidence_classifier.bin";
    private static final double probabilityTol = 1.0e-7;

    private final ClassifierTestData testData = new ClassifierTestData(testDataJsonFile);
    private final double[] predictYProbaSerial = predictProba(
            XGBoostEvidenceFilter.loadPredictor(localClassifierModelFile), testData.features
    );

    @Test(groups = "sv")
    protected void testLocalXGBoostClassifierAccuracy() {
        // check accuracy: predictions are same as testData up to tolerance
        Assert.assertArrayEquals("Probabilities predicted by classifier do not match saved correct answers",
                predictYProbaSerial, testData.yProba, probabilityTol);
    }

    @Test(groups = "sv")
    protected void testLocalXGBoostClassifierSpark() {
        final Predictor localPredictor = XGBoostEvidenceFilter.loadPredictor(localClassifierModelFile);
        // get spark ctx
        final JavaSparkContext ctx = SparkContextFactory.getTestSparkContext();
        // parallelize testData to RDD
        JavaRDD<FVec> testFeaturesRdd = ctx.parallelize(Arrays.asList(testData.features));
        // predict in parallel
        JavaDoubleRDD predictYProbaRdd
                = testFeaturesRdd.mapToDouble(f -> localPredictor.predictSingle(f, false, 0));
        // pull back to local array
        final double[] predictYProbaSpark = predictYProbaRdd.collect()
                .stream().mapToDouble(Double::doubleValue).toArray();
        // check probabilities from spark are identical to serial
        Assert.assertArrayEquals("Probabilities predicted in spark context differ from serial",
                predictYProbaSpark, predictYProbaSerial, 0.0);
    }

    @Test(groups = "sv")
    protected void testResourceXGBoostClassifier() {
        // load classifier from resource
        final Predictor resourcePredictor = XGBoostEvidenceFilter.loadPredictor(resourceClassifierModelFile);
        final double[] resourceYProba = predictProba(resourcePredictor, testData.features);
        // check that predictions from resource are identical to local
        Assert.assertArrayEquals("Predictions via loading predictor from resource is not identical to local file",
                resourceYProba, predictYProbaSerial, 0.0);
    }

    private static double[] predictProba(final Predictor predictor, final FVec[] testFeatures) {
        final int numData = testFeatures.length;
        final double[] yProba = new double[numData];
        for(int rowIndex = 0; rowIndex < numData; ++rowIndex) {
            yProba[rowIndex] = predictor.predictSingle(testFeatures[rowIndex], false,0);
        }
        return yProba;
    }

    private static class ClassifierTestData {
        final EvidenceFeatures[] features;
        final double[] yProba;

        ClassifierTestData(final String jsonFileName) {
            try(final InputStream inputStream = new FileInputStream(jsonFileName)) {
                final JsonNode testDataNode = new ObjectMapper().readTree(inputStream);
                features = getFVecArrayFromJsonNode(testDataNode.get("X"));
                yProba = getDoubleArrayFromJsonNode(testDataNode.get("y_proba"));
            } catch(Exception e) {
                throw new GATKException(
                        "Unable to load classifier test data from " + jsonFileName + ": " + e.getMessage()
                );
            }
        }

        private static EvidenceFeatures[] getFVecArrayFromJsonNode(final JsonNode matrixNode) {
            if(!matrixNode.has("__class__")) {
                throw new IllegalArgumentException("JSON node does not store python matrix data");
            }
            String matrixClass = matrixNode.get("__class__").asText();
            switch(matrixClass) {
                case "pandas.DataFrame":
                    return getFVecArrayFromPandasJsonNode(matrixNode.get("data"));
                case "numpy.array":
                    return getFVecArrayFromNumpyJsonNode(matrixNode.get("data"));
                default:
                    throw new IllegalArgumentException("JSON node has __class__ = " + matrixClass
                            + "which is not a supported matrix type");
            }
        }

        private static EvidenceFeatures[] getFVecArrayFromNumpyJsonNode(final JsonNode dataNode) {
            if(!dataNode.isArray()) {
                throw new IllegalArgumentException("dataNode does not encode a valid numpy array");
            }
            final int numRows = dataNode.size();
            final EvidenceFeatures[] matrix = new EvidenceFeatures[numRows];
            if (numRows == 0) {
                return matrix;
            }
            matrix[0] = new EvidenceFeatures(getDoubleArrayFromJsonArrayNode(dataNode.get(0)));
            final int numColumns = matrix[0].length();
            for (int row = 1; row < numRows; ++row) {
                matrix[row] = new EvidenceFeatures(getDoubleArrayFromJsonArrayNode(dataNode.get(row)));
                final int numRowColumns = matrix[row].length();
                if (numRowColumns != numColumns) {
                    throw new IllegalArgumentException("Rows in JSONArray have different lengths.");
                }
            }
            return matrix;
        }

        private static EvidenceFeatures[] getFVecArrayFromPandasJsonNode(final JsonNode dataNode) {
            if(!dataNode.isObject()) {
                throw new IllegalArgumentException("dataNode does not encode a valid pandas DataFrame");
            }
            final int numColumns = dataNode.size();
            if(numColumns == 0) {
                return new EvidenceFeatures[0];
            }

            final String firstColumnName = dataNode.fieldNames().next();
            final int numRows = getColumnArrayNode(dataNode.get(firstColumnName)).size();
            final EvidenceFeatures[] matrix = new EvidenceFeatures[numRows];
            if (numRows == 0) {
                return matrix;
            }
            // allocate each EvidenceFeatures in matrix
            for(int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
                matrix[rowIndex] = new EvidenceFeatures(numColumns);
            }
            int columnIndex = 0;
            for(final Iterator<Map.Entry<String, JsonNode>> fieldIter = dataNode.fields(); fieldIter.hasNext();) {
                // loop over columns
                final Map.Entry<String, JsonNode> columnEntry = fieldIter.next();
                final JsonNode columnArrayNode = getColumnArrayNode(columnEntry.getValue());
                if(columnArrayNode.size() != numRows) {
                    throw new IllegalArgumentException("field " + columnEntry.getKey() + " has "
                            + columnArrayNode.size() + " rows (expected " + numRows + ")");
                }
                // for each FVec in matrix, assign feature from this column
                int rowIndex = 0;
                for(final JsonNode valueNode: columnArrayNode) {
                    final EvidenceFeatures fVec = matrix[rowIndex];
                    fVec.set_value(columnIndex, valueNode.asDouble());
                    ++rowIndex;
                }
                ++columnIndex;
            }
            return matrix;
        }

        private static JsonNode getColumnArrayNode(final JsonNode columnNode) {
            return columnNode.has("values") ? columnNode.get("values") : columnNode.get("codes");
        }

        private static double[] getDoubleArrayFromJsonNode(final JsonNode vectorNode) {
            if(!vectorNode.has("__class__")) {
                throw new IllegalArgumentException("JSON node does not store python matrix data");
            }
            String vectorClass = vectorNode.get("__class__").asText();
            switch(vectorClass) {
                case "pandas.Series":
                    return getDoubleArrayFromJsonArrayNode(getColumnArrayNode(vectorNode));
                case "numpy.array":
                    return getDoubleArrayFromJsonArrayNode(vectorNode.get("data"));
                default:
                    throw new IllegalArgumentException("JSON node has __class__ = " + vectorClass
                            + "which is not a supported matrix type");
            }
        }

        private static double [] getDoubleArrayFromJsonArrayNode(final JsonNode arrayNode) {
            if(!arrayNode.isArray()) {
                throw new IllegalArgumentException("JsonNode does not contain an Array");
            }
            final int numData = arrayNode.size();
            final double[] data = new double[numData];
            int ind = 0;
            for(final JsonNode valueNode : arrayNode) {
                data[ind] = valueNode.asDouble();
                ++ind;
            }
            return data;
        }
    }
}
