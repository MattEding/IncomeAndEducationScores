import pickle
from importlib import resources

from sklearn.metrics import mean_squared_error

from eduscores.ml import train

with resources.path('eduscores.data', '') as path:
    PKL_DIR = path / 'pkl'


def main(modelfile):
    with open(PKL_DIR / modelfile, 'rb') as fp:
        model = pickle.load(fp)

    _, X_test, _, y_test = train.load_data()
    
    y_pred = model.predict(X_test)
    score = mean_squared_error(y_test, y_pred)
    print(score)
