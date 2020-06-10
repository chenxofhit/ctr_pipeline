import xgboost as xgb
import pandas as pd
import gc
from sklearn.metrics import roc_auc_score
import numpy as np


class XgboostIncrementalModel(object):
    """
    Incremental learning of Xgboost model
    TODO: check details
    """
    def __init__(self, hparams, train_dir, val_dir, save_model_dir, drop_cols=None, label_col='click',
                 batch_size=10000000, epochs=10, num_boost_round=1000, evaluate_steps=1, save_steps=10,
                 shuffle=True, gc_memory=False):
        """

        Args:
            hparams(dict): hyper parameters of model
            train_dir(str): dir of train dataset
            val_dir(str): dir of val dataset
            save_model_dir(str): dir of save model folder
            drop_cols(list): list of column names which need to be dropped before training
            label_col(str): name of label column
            batch_size(int): batch size
            epochs(int): num of training epoch
            num_boost_round(int): num rounds of boosting for xgboost model
            evaluate_steps(int): interval steps between evaluating
            save_steps(int): interval steps between saving model
            shuffle(bool): True for shuffle in each batch
            gc_memory(bool): True for gc useless and large intermediate variable
        """
        self.hparams = hparams
        self.train_dir = train_dir
        self.val_dir = val_dir
        self.save_model_dir = save_model_dir
        self.batch_size = batch_size
        self.epochs = epochs
        self.drop_cols = drop_cols
        self.label_col = label_col
        self.num_boost_round = num_boost_round
        self.evaluate_steps = evaluate_steps
        self.save_steps = save_steps
        self.shuffle = shuffle
        self.gc_memory = gc_memory
        # init xgb model
        self._summary = []
        self._models = []
        if not self.save_model_dir.endswith('/'):
            self.save_model_dir += '/'

    def train_and_evaluate(self):
        """
        Train and evaluate xgboost model
        Returns:

        """
        # TODO use usecols to replace read then drop cols
        reader = pd.read_csv(self.train_dir, chunksize=self.batch_size)
        val_df = pd.read_csv(self.val_dir)

        if self.drop_cols:
            val_df = val_df.drop(self.drop_cols, axis=1)

        y_val = val_df[self.label_col].values
        x_val = val_df.drop([self.label_col], axis=1).values
        val_dmatrix = xgb.DMatrix(x_val)
        if self.gc_memory:
            del val_df, x_val
            gc.collect(generation=2)

        global_steps = 0
        print('Start Training model:')
        print('Save model path: {}'.format(self.save_model_dir))
        for epoch in range(self.epochs):
            for df_batch in reader:

                if self.drop_cols:
                    df_batch = df_batch.drop(self.drop_cols, axis=1)

                if self.shuffle:
                    df_batch = df_batch.sample(frac=1)

                y_batch = df_batch[self.label_col].values
                x_batch = df_batch.drop([self.label_col], axis=1).values
                train_dmatrix = xgb.DMatrix(x_batch, y_batch)
                if self.gc_memory:
                    del df_batch,x_batch,y_batch
                    gc.collect(generation=2)
                if global_steps == 0:
                    model = xgb.train(params=self.hparams,
                                      dtrain=train_dmatrix,
                                      num_boost_round=self.num_boost_round)
                else:
                    model = xgb.train(params=self.hparams,
                                      dtrain=train_dmatrix,
                                      xgb_model=self._models[-1],
                                      num_boost_round=self.num_boost_round)
                self._models.append(model)

                if global_steps % self.evaluate_steps == 0:
                    metric = self._evaluate(model, val_dmatrix, y_val)
                    self._summary.append(metric)
                    print('Step-{}: Auc-val: {.5f}'.format(global_steps, metric))

                if global_steps % self.save_steps:
                    model.save_model('{}xgb_{}.model'.format(self.save_model_dir, global_steps))

                global_steps += 1
        self._summary = np.array(self._summary)
        best_metric = self._summary.max()
        self.best_steps = self._summary.argmax()
        print('Best Auc-val: {.5f} , Steps:{}'.format(best_metric, self.best_steps))
        self._models[self.best_steps].save_model('{}xgb_{}_best.model'.format(self.save_model_dir, global_steps))
        print('Save best model file to {}'.format(self.save_model_dir))

    def _evaluate(self, model, x_dmatrix, y_true):

        pred = model.predict(x_dmatrix)

        metric = roc_auc_score(y_true, pred)
        return metric

    # def save_model(self, save_dir,num_steps='best'):
    #     if num_steps == 'best':
    #         self._models[self.best_steps].save_model(save_dir)
