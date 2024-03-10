# <YOUR_IMPORTS>
import dill
import json
import pandas as pd
import os
import codecs

path = os.environ.get('PROJECT_PATH', 'C:/Users/HUAWEI/airflow_hw')
with open(f'{path}/data/models/cars_pipe_1.dill', 'rb') as file:
    model = dill.load(file)
files = []
for file in os.listdir(f"{path}/data/test"):
    if file.endswith(".json"):
        files.append(os.path.join(f"{path}/data/test", file))
predict_list = []

def prediction():
    for file in files:
        with codecs.open(file) as f:
            tweets = json.load(f)
            df = pd.DataFrame.from_dict([tweets])
            y = model.predict(df)
            print(y)
            predict_list.append(y)
    df2 = pd.DataFrame.from_dict(predict_list)

    df2.to_csv(f'{path}/data/predictions/predict_list.csv')






if __name__ == '__main__':
    prediction()

