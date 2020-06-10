class Model(object):
    def __init__(self):
        pass

    def train_and_evaluate(self):
        raise NotImplementedError

    def predict(self):
        raise NotImplementedError

    def save_model(self):
        raise NotImplementedError
