# Flight Price Prediction

Showcase Pyspark project that process 20Gb of flight price data and uses a ML model to train, test and predict flight prices.

## Source Data
Download the source data or use it as sample for data schema:
https://www.kaggle.com/datasets/dilwong/flightprices

## Usage
The scripts utilizes PySpark 2.3.1 so make sure you have it installed in your enviroment. Other dependencies please see requirements.txt

spark-submit main.py [train/predict] --input "[path_to_your_training_testing_data_file]" --model_path "[path_to_model_directory]" --output["path_to_save_prediction_results"]