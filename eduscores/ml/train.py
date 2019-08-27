import pickle
from importlib import resources

import numpy as np
import pandas as pd

from sklearn.dummy import DummyRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, Lasso, Ridge
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from eduscores import logs


LOGGER = logs.get_logger(__name__)
TARGET = 'median_household_income'

with resources.path('eduscores.data', '') as path:
    PKL_DIR = path / 'pkl'


def load_data(test_size=0.2):
    df = pd.read_pickle(PKL_DIR / 'eduscore.pkl').dropna()
    # drop columns with no inherent meaning
    X = df.drop(columns=[TARGET, 'cds_id', 'zipcode'])
    y = np.log(df[TARGET])
    return train_test_split(X, y, test_size=test_size, random_state=0)
    

def pipeline_cv(regressor, params, scoring=None, cv=5, n_jobs=1):
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('regressor', regressor),
    ])
    
    grid_search = GridSearchCV(pipeline, params, cv=cv, n_jobs=n_jobs, scoring=scoring)
    return grid_search


def get_feature_importance(filename, sort=False):
    names = load_data()[0].columns
    with open(PKL_DIR / filename, 'rb') as fp: 
        model = pickle.load(fp) 
    
    reg = model['regressor'] 
    try:
        feats = reg.coef_
    except AttributeError:
        feats = reg.feature_importances_
    features = zip(names, feats)
    if sort:
        return sorted(features, key=lambda x: abs(x[1]), reverse=True)
    return list(features)


def main(*, test_size=0.2, n_jobs=1, loglevel='INFO'):
    LOGGER.setLevel(loglevel.upper())

    X_train, _, y_train, _ = load_data()

    models = {
        # Baseline Models
        DummyRegressor: dict(
            regressor__strategy=['mean', 'median'],
        ),
        # Linear Models
        LinearRegression: dict(),
        Lasso: dict(
            regressor__alpha=[10**i for i in range(-3, 4)],
            regressor__max_iter=[2000],
            regressor__random_state=[0],
        ),
        Ridge: dict(
            regressor__alpha=[10**i for i in range(-3, 4)],
            regressor__max_iter=[2000],
            regressor__random_state=[0],
        ),
        # Ensembles
        RandomForestRegressor: dict(
            regressor__n_estimators=[100],
            regressor__random_state=[0],
        ),
    }
    
    scoring = 'neg_mean_squared_error'

    for model, params in models.items():
        pipe_grid = pipeline_cv(model(), params, scoring=scoring, n_jobs=n_jobs)
        pipe_grid.fit(X_train, y_train)

        LOGGER.info(f'model: {model.__name__}, score: {pipe_grid.best_score_}')
        LOGGER.debug(f'\tparams: {pipe_grid.best_params_}')

        filename = f'{model.__name__}.pkl'
        with open(PKL_DIR / filename, 'wb') as fp:
            pickle.dump(pipe_grid.best_estimator_, fp)