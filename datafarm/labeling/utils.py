"""Small helpers shared by the labeling modules."""


def get_X_y(df, feature_cols, label_col):
    return df.loc[:, feature_cols].values, df.loc[:, label_col].values
