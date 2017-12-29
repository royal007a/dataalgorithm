 This example focuses on predicting car prices using 10 car features (such as Price, Age ,FuelType, ...). 
 使用10个car的特征，，，，
 Training Data  =》 convert and transformer and 
 featurization   =》 , we do need to transform non-numeric data (such FuelType) into a numeric value
 awk -f ./scripts/transform.awk ./resources/ToyotaCorolla.csv > ./resources/ToyotaCorolla_Transformed.csv
 
 建立模型 buildmodel The class CarPricePredictionBuildModel builds the model from the given training data
 
预测结果  Examine the Built Model  Using LinearRegressionModel
 CarPricePrediction This is the driver class, which uses the built model to predict new queried cars

检验准确性 Output from ModelEvaluation
 This is the test class, which tests the accuracy of the built model for predicting new queried cars. 
We will test the model against the training data, which the model is built from originally.