from deepctr.models.esmm import ESMM
from deepctr.inputs import DenseFeat, SparseFeat, VarLenSparseFeat
import numpy as np
import tensorflow as tf


if __name__ == "__main__":

    user_feature_columns = [
        DenseFeat('pay_score', 1),
        SparseFeat('user', 4, embedding_dim=4),
        SparseFeat('gender', 2, embedding_dim=4),
        VarLenSparseFeat(
            SparseFeat('hist_item_id', 3 + 1, embedding_dim=4,embedding_name='item_id'),
            maxlen=4, 
            # length_name="seq_length"
        ),
        VarLenSparseFeat(
            SparseFeat('hist_cate_id', 2 + 1, embedding_dim=4,embedding_name='cate_id'),
            maxlen=4, 
            # length_name="seq_length"
        )]

    item_feature_columns = [
        SparseFeat('item_id', 3 + 1, embedding_dim=4),
        SparseFeat('cate_id', 2 + 1, embedding_dim=4),
    ]


    X = {
        'user':     np.array([0, 1, 2, 3]), 
        'gender':   np.array([0, 1, 0, 1]), 
        'item_id':  np.array([1, 2, 3, 2]) , 
        'cate_id':  np.array([1, 2, 1, 2]),
        'hist_item_id': np.array([[1, 2, 3, 0], [1, 3, 2, 1], [3, 2, 0, 0], [2, 1, 0, 0]]), 
        'hist_cate_id': np.array([[1, 1, 2, 0], [2, 1, 1, 2], [1, 2, 0, 0], [2, 1, 0, 0]]),
        'pay_score': np.array([0.1, 0.2, 0.3, 0.2]), 
        # 'seq_length': np.array([3, 4, 2, 2])
    }

    y_ctr = np.array([1, 1, 1, 0])
    y_ctcvr = np.array([1, 0, 1, 0])

    tf.compat.v1.disable_eager_execution() # tf.keras.backend.clear_session()
    model = ESMM(user_feature_columns, item_feature_columns)

    model.compile(optimizer='rmsprop', 
                  loss='binary_crossentropy',
                  metrics=['binary_crossentropy'],
                  loss_weights=[1., 1.])

    history = model.fit(X, [y_ctr, y_ctcvr],
        batch_size=2, epochs=2, verbose=2, validation_split=0.25, )
    


    ###########################################

    # model_save_path = "./model/"
    # version = "1"
    # model.save(model_save_path + version, save_format="tf")
