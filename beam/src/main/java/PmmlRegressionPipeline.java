import com.google.common.io.Resources;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.LoadingModelEvaluatorBuilder;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

/**
 * Example pipeline for applying a model specified as PMML to data read from csv file.
 * The pipeline has the following steps:
 * - Load data from csv file
 * - Apply the PMML specification to the data in each row
 * - Save the results back to csv file
 */
public class PmmlRegressionPipeline {
    private static final String[] COLUMN_NAMES =
        {"year", "plurality", "apgar_5min", "mother_age", "father_age", "gestation_weeks", "ever_born", "mother_married", "weight_pounds"};
    private static final String TARGET_COLUMN = "weight_pounds";

    /**
     * Creates and runs a pipeline that reads data from csv file, applies a PMML model to make
     * a prediction, and write the results to csv file.
     */
    public static void main(String[] args) throws JAXBException, SAXException, IOException {
        // create the data flow pipeline
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        // load the PMML model
        final Evaluator evaluator;
        URL r = Resources.getResource("natality.pmml");
        evaluator = new LoadingModelEvaluatorBuilder()
            .load(new File(r.getPath()))
            .build();

        // get the data from csv file
        pipeline
            .apply(
                TextIO.read()
                    .from(options.getSrc()))
            // apply the PMML model to each of the records in the result set
            .apply("PMML Application",
                new PTransform<PCollection<String>, PCollection<String>>() {

                    // define a transform that loads the PMML specification and applies it to all of the records
                    public PCollection<String> expand(PCollection<String> input) {

                        // create a DoFn for applying the PMML model to instances
                        return input.apply("To Predictions", ParDo.of(new DoFn<String, String>() {

                            @ProcessElement
                            public void processElement(ProcessContext c) throws Exception {
                                String line = c.element();
                                if (line.contains(COLUMN_NAMES[0])) {
                                    return;
                                }

                                String[] parts = line.split(",");
                                // create a map of inputs for the pmml model
                                HashMap<FieldName, Double> inputs = new HashMap<>();
                                for (int i = 0; i < COLUMN_NAMES.length; i++) {
                                    String key = COLUMN_NAMES[i];
                                    if (key.equals(TARGET_COLUMN)) {
                                        continue;
                                    }

                                    inputs.put(FieldName.create(key), Double.parseDouble(parts[i]));
                                }

                                // get the estimate
                                Double estimate = (Double) evaluator.evaluate(inputs).get(FieldName.create(TARGET_COLUMN));

                                // create a table row with the prediction
                                String result = line + "," + estimate.toString();

                                // output the prediction to the data flow pipeline
                                c.output(result);
                            }
                        }));
                    }
                })
            // write the results to csv file
            .apply(
                TextIO.write()
                    .to(options.getDestPrefix())
                    .withNumShards(1)
                    .withSuffix(".csv"));

        // run it
        pipeline.run();
    }

    /**
     * Provide an interface for setting the GCS temp location
     */
    public interface Options extends PipelineOptions {
        @Validation.Required
        @Description("File path to csv file for input E.g.) gs://some-bucket/natality-eval.csv")
        ValueProvider<String> getSrc();

        void setSrc(ValueProvider<String> src);

        @Validation.Required
        @Description("Prefix string of file path to csv file for output E.g.) gs://some-bucket/out/predicted_weights")
        ValueProvider<String> getDestPrefix();

        void setDestPrefix(ValueProvider<String> destPrefix);

        String getTempLocation();

        void setTempLocation(String value);
    }
}