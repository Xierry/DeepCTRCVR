import tensorflow as tf
from ..inputs import SparseFeat, VarLenSparseFeat, DenseFeat, \
    create_embedding_matrix, build_input_features, embedding_lookup, varlen_embedding_lookup, \
        get_varlen_pooling_list, combined_dnn_input
from ..layers.utils import concat_func, reduce_sum
from ..layers.core import DNN, PredictionLayer


def ESMM(user_feature_columns, item_feature_columns, other_feature_columns=None, mask_feat_list=(), 
         dnn_hidden_units = (200, 80), dnn_activation="relu", dnn_dropout=0.0, dnn_use_bn=False, 
         l2_reg_embedding=1e-6, dnn_l2_reg=0.0, init_std=0.0001, task='binary', seed=1024):

    # 嵌入字典
    embedding_dict = create_embedding_matrix(
        user_feature_columns + item_feature_columns,
        l2_reg_embedding, init_std, seed, seq_mask_zero=True)

    user_input_dict = build_input_features(user_feature_columns) # -> (None, 1), (None, E)
    item_input_dict = build_input_features(item_feature_columns) # -> (None, 1)

    user_inputs_list = list(user_input_dict.values())
    item_inputs_list = list(item_input_dict.values())

    user_field_pooled, user_dense_value_list = get_emb_list(user_feature_columns, embedding_dict, user_input_dict, mask_feat_list)
    item_field_pooled, item_dense_value_list = get_emb_list(item_feature_columns, embedding_dict, item_input_dict, mask_feat_list)

    dnn_input = combined_dnn_input([user_field_pooled, item_field_pooled], 
        user_dense_value_list + item_dense_value_list) # -> (None, E_dnn)

    # pCVR part
    dnn_output = DNN(dnn_hidden_units, dnn_activation, dnn_l2_reg, dnn_dropout,
                     dnn_use_bn, seed)(dnn_input) # (None, 64)
    logit = tf.keras.layers.Dense(1, use_bias=False)(dnn_output)
    pcvr = PredictionLayer(task)(logit)

    # pCTR part
    dnn_output = DNN(dnn_hidden_units, dnn_activation, dnn_l2_reg, dnn_dropout,
                     dnn_use_bn, seed)(dnn_input)
    logit = tf.keras.layers.Dense(1, use_bias=False)(dnn_output)
    pctr = PredictionLayer(task)(logit)

    # pCTCVR part
    pctcvr = tf.multiply(pctr, pcvr)

    model = tf.keras.models.Model(inputs=user_inputs_list+item_inputs_list, outputs=[pctr, pctcvr])

    return model


def get_emb_list(feature_columns, embedding_dict, input_dict, mask_feat_list):

    sparse_feature_columns = [fc for fc in feature_columns if isinstance(fc, SparseFeat)]
    varlen_sparse_feature_columns = [fc for fc in feature_columns if isinstance(fc, VarLenSparseFeat)]
    dense_value_list = [input_dict[fc.name] for fc in feature_columns if isinstance(fc, DenseFeat)]

    sparse_emb_list = embedding_lookup(embedding_dict, input_dict, sparse_feature_columns, 
        mask_feat_list=mask_feat_list, to_list=True)

    varlen_sparse_emb_dict = varlen_embedding_lookup(
        embedding_dict, input_dict, varlen_sparse_feature_columns)
    varlen_sparse_emb_list = get_varlen_pooling_list(
        varlen_sparse_emb_dict, input_dict, varlen_sparse_feature_columns, to_list=True)
                               
    # [None, num_feat, E]  <- (None, 1, E)       (None, 1, E)
    emb_concated = concat_func(sparse_emb_list + varlen_sparse_emb_list, axis=-2)
    field_pooled = reduce_sum(emb_concated, axis=-2) # -> [None, E]

    return field_pooled, dense_value_list


if __name__ == "__main__":
    import numpy as np
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
        'seq_length': np.array([3, 4, 2, 2])
    }

    y_ctr = np.array([1, 1, 1, 0])
    y_ctcvr = np.array([1, 0, 1, 0])