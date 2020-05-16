import tensorflow as tf
from ..inputs import SparseFeat, VarLenSparseFeat, DenseFeat, \
    create_embedding_matrix, build_input_features, embedding_lookup, varlen_embedding_lookup, \
        get_varlen_pooling_list, combined_dnn_input
from ..layers.utils import concat_func, reduce_sum
from ..layers.core import DNN, PredictionLayer


def ESMM(user_feature_columns, item_feature_columns, context_feature_columns=None, mask_feat_list=(), 
         dnn_hidden_units = (200, 80), dnn_activation="relu", dnn_dropout=0.0, dnn_use_bn=False, 
         l2_reg_embedding=1e-6, dnn_l2_reg=0.0, init_std=0.0001, task='binary', seed=1024):

    # 嵌入字典
    feature_columns = user_feature_columns + item_feature_columns
    if context_feature_columns is not None:
        feature_columns += context_feature_columns
    embedding_dict = create_embedding_matrix(
        feature_columns, l2_reg_embedding, init_std, seed, seq_mask_zero=True)

    user_input_dict = build_input_features(user_feature_columns) # -> (None, 1), (None, E)
    item_input_dict = build_input_features(item_feature_columns) # -> (None, 1)

    user_inputs_list = list(user_input_dict.values())
    item_inputs_list = list(item_input_dict.values())

    user_field_sum, user_dense_value_list = get_emb_list(user_feature_columns, embedding_dict, user_input_dict, mask_feat_list)
    item_field_sum, item_dense_value_list = get_emb_list(item_feature_columns, embedding_dict, item_input_dict, mask_feat_list)

    embedding_list = [user_field_sum, item_field_sum]
    dense_value_list = user_dense_value_list + item_dense_value_list
    if context_feature_columns is not None:
        context_input_dict = build_input_features(context_feature_columns) # -> (None, 1)
        context_inputs_list = list(context_input_dict.values())
        context_field_sum, context_dense_value_list = get_emb_list(context_feature_columns, embedding_dict, context_input_dict, mask_feat_list)
        embedding_list.append(context_field_sum)
        dense_value_list += context_dense_value_list

    dnn_input = combined_dnn_input(embedding_list, dense_value_list) # -> (None, E_dnn)

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

    inputs = user_inputs_list+item_inputs_list
    if context_feature_columns is not None:
        inputs += context_inputs_list
    model = tf.keras.models.Model(
        inputs=inputs, outputs=[pctr, pctcvr])

    pcvr_model = tf.keras.models.Model(
        inputs=inputs, outputs=[pcvr])

    # model.get_layer("input_name").get_weights()
    return model, pcvr_model


def get_emb_list(feature_columns, embedding_dict, input_dict, mask_feat_list):

    sparse_feature_columns = [fc for fc in feature_columns if isinstance(fc, SparseFeat)]
    varlen_sparse_feature_columns = [fc for fc in feature_columns if isinstance(fc, VarLenSparseFeat)]
    dense_value_list = [input_dict[fc.name] for fc in feature_columns if isinstance(fc, DenseFeat)]

    sparse_emb_list = embedding_lookup(embedding_dict, input_dict, sparse_feature_columns, 
        mask_feat_list=mask_feat_list, to_list=True)

    varlen_sparse_emb_dict = varlen_embedding_lookup(
        embedding_dict, input_dict, varlen_sparse_feature_columns)
    varlen_sparse_emb_list = list(get_varlen_pooling_list(
        varlen_sparse_emb_dict, input_dict, varlen_sparse_feature_columns, to_list=True))
                      
    # [None, num_feat, E]  <- (None, 1, E)       (None, 1, E)
    emb_concated = concat_func(sparse_emb_list + varlen_sparse_emb_list, axis=-2)
    field_sum = reduce_sum(emb_concated, axis=-2) # -> [None, E]

    return field_sum, dense_value_list
