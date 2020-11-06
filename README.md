# pmml-r-beam-example

# About
This project uses the PMML model trained in R to make predictions with Apache Beam. 

Based on the article at https://towardsdatascience.com/productizing-ml-models-with-dataflow-99a224ce9f19 and the code at https://github.com/bgweber/StartupDataScience/tree/master/misc, updated dependencies to the latest.

# Usage
## Train a linear regression model with R
- Run the R script file at `r/pmml.R`.
- The following 2 files will be output as a result.
    - `beam/src/main/resources/natality.pmml`: PMML model file
    - `r/predicted_weights_by_r.csv`: predicted values with the model

## Apply the PMML model to predict the value with Apache Beam
- Run the following maven command in the terminal to make predictions with the Beam application.
```shell script
$ mvn clean compile exec:java -Dexec.mainClass=PmmlRegressionPipeline \
     -Dexec.args="--src=../data/natality-eval.csv \
     --destPrefix=../r/predicted_weights_by_beam" -Pdirect-runner
```
- The predicted values will be output in the following file.
    - `r/predicted_weights_by_beam-00000-of-00001.csv`
