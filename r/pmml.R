# Preparing the training data
df_train <-
  as.data.frame(read.csv("../data/natality-train.csv"))

summary(df_train)
hist(df_train$weight_pounds)

# Training the Model
lm <- lm(weight_pounds ~ ., data = df_train)
summary(lm)

# Translating to PMML
#install.packages("r2pmml")
library(r2pmml)
r2pmml(lm, "../beam/src/main/resources/natality.pmml")

# Preparing the test data
df_eval <- read.csv("../data/natality-eval.csv")

# Appling the model prediction
y <- predict(lm, df_eval)
df_eval$predicted <- y

# Writing the predicted weight values
write.csv(df_eval, "predicted_weights_by_r.csv", row.names = FALSE)
