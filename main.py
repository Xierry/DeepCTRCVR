from deepctr.models.esmm import ESMM
from deepctr.inputs import DenseFeat, SparseFeat, VarLenSparseFeat
import numpy as np
import tensorflow as tf


if __name__ == "__main__":
    embedding_dim = 4
    n_copy = 2
    X = {
        'user':     np.array([0, 1, 2, 3] * n_copy),
        'gender':   np.array([0, 1, 0, 1] * n_copy),
        'item_id':  np.array([1, 2, 3, 2] * n_copy), 
        'cate_id':  np.array([1, 2, 1, 2] * n_copy),
        'hist_item_id': np.array([[1, 2, 3, 0], [1, 3, 2, 1], [3, 2, 0, 0], [2, 1, 0, 0]] * n_copy), 
        'hist_cate_id': np.array([[1, 1, 2, 0], [2, 1, 1, 2], [1, 2, 0, 0], [2, 1, 0, 0]] * n_copy),
        'pay_score': np.array([0.1, 0.2, 0.3, 0.2] * n_copy),
        'context':  np.array([0, 1, 0, 1] * n_copy),
        # 'seq_length': np.array([3, 4, 2, 2])
    }

    y_ctr = np.array(  [1, 1, 1, 0] * n_copy)
    y_cvr = np.array(  [1, 0, 1, 0] * n_copy)
    y_ctcvr = np.array([1, 0, 1, 0] * n_copy)
    
    # 用户特征
    user_feature_columns = [
        DenseFeat('pay_score', dimension = 1),

        SparseFeat('user', vocabulary_size=len(np.unique(X["user"])), 
                           embedding_dim=embedding_dim),
        SparseFeat('gender', vocabulary_size=len(np.unique(X["gender"])), 
                             embedding_dim=embedding_dim),

        VarLenSparseFeat( # 0 值表示填充, 自动过滤
            SparseFeat('hist_item_id',
                vocabulary_size=len(np.unique(X["hist_item_id"][0])),
                embedding_dim=embedding_dim, 
                embedding_name='item_id'),
            maxlen=len(X["hist_item_id"][0]), 
            combiner="max", # "mean", "sum"
            length_name=None,# length_name="seq_length"
        ),
        VarLenSparseFeat(
            SparseFeat('hist_cate_id', len(np.unique(X["hist_cate_id"][0])), 
                embedding_dim=embedding_dim, 
                embedding_name='cate_id'),
            maxlen=len(X["hist_cate_id"][0]), 
            combiner="max", # "mean", "sum" 
            length_name=None, # length_name="seq_length"
        )]

    item_feature_columns = [ # 多了 0 填充, 用行为序列的唯一值
        SparseFeat('item_id', 
            vocabulary_size=len(np.unique(X["hist_item_id"][0])), 
            embedding_dim=embedding_dim),
        SparseFeat('cate_id', 
        vocabulary_size=len(np.unique(X["hist_cate_id"][0])), 
        embedding_dim=embedding_dim),
    ]

    context_feature_columns = [
        SparseFeat('context', vocabulary_size=2, 
                              embedding_dim=embedding_dim),]
    
    # 兼容旧版本
    tf.compat.v1.disable_eager_execution() # tf.keras.backend.clear_session()

    model, pcvr_model = ESMM(user_feature_columns, item_feature_columns, context_feature_columns)

    model.compile(optimizer='rmsprop', 
                  loss='binary_crossentropy',
                  metrics=['binary_crossentropy'],
                  loss_weights=[.5, .5])

    history = model.fit(X, [y_ctr, y_ctcvr],
        batch_size=3, epochs=20, verbose=2, validation_split=0.25)

    pcvr = pcvr_model.predict(X) # 模型预测


    ###########################################

    # model_save_path = "./model/"
    # version = "1"
    # model.save(model_save_path + version, save_format="tf")
