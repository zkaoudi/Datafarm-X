"""Statistics of the IMDB (Kaggle) tables (filter value quantiles, distinct group-by values) for the job generator.

Usage: python datafarm/datasets_metadata/imdb/MetaDataAnalyzer.py <folder with the preprocessed <table>.csv files>
"""
import pandas as pd
import numpy as np
import argparse
import sys
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))))  # repository root
from datafarm.datasets_metadata.imdb import TableMetaData  # noqa: E402



qs = [0.25, 0.5, 0.75, 0.95, 0.97, 0.99]


def analyze_filter_field(ff, data_df):
    f_dict = {}

    # dtype checks that also hold for pandas >= 3 (text columns are "str", dates may not be in ns)
    if pd.api.types.is_datetime64_any_dtype(data_df[ff]):
        f_df = data_df[ff].astype("datetime64[ns]").astype(np.int64).quantile(qs)
        f_dict["selectivity"] = [str(x) for x in f_df.index.values.tolist()]
        f_dict["values"] = [str(x) for x in pd.to_datetime(f_df).apply(lambda x: x.strftime("%Y-%m-%d")).values.tolist()]

    elif not pd.api.types.is_numeric_dtype(data_df[ff]):  # text
        f_df = (data_df.groupby(ff).size() / data_df.__len__())
        f_dict["selectivity"] = [str(x) for x in f_df.values.tolist()]
        f_dict["values"] = [str(x) for x in f_df.index.values.tolist()]

    else:
        f_df = data_df[ff].quantile(qs)
        f_dict["selectivity"] = [str(x) for x in f_df.index.values.tolist()]
        f_dict["values"] = [str(x) for x in f_df.values.tolist()]

    return f_dict


def analyze_filter_field_2(ff, data_df, f_values):
    f_dict = {}

    if not pd.api.types.is_numeric_dtype(f_values):  # text
        f_dict["selectivity"] = []
        f_dict["values"] = []
        for v in f_values.values:
            selectivity = data_df[data_df[ff].str.contains(v).fillna(False)].__len__() / data_df.__len__()
            f_dict["selectivity"].append(str(selectivity))
            f_dict["values"].append(v)
    else:
        raise Exception()

    return f_dict


def analyze_groupby_field(gf, data_df):
    return data_df[gf].nunique()


def toScala(s):
    s = s.replace("{", "Map(")
    s = s.replace("}", ")")
    s = s.replace("[", "Seq(")
    s = s.replace("]", ")")
    s = s.replace(":", "->")
    s = s.replace("'", '"')
    return s


def main(data_source):
    db_meta_dict = TableMetaData.database_meta

    for table_n, table_info_dict in db_meta_dict.items():
        print(f"| Analyzing '{table_n}'")

        data_df = pd.read_csv(_os.path.join(data_source, table_n + ".csv"), sep=";", header=None, names=table_info_dict["fields"], index_col=False,
                              parse_dates=table_info_dict["date_fields"])

        print(data_df.info())
        print(data_df.head(10))

        print("| JOIN FIELDS")
        print(f"|  {db_meta_dict[table_n]['join_fields']}")
        print("Scala:")
        print(toScala(str(db_meta_dict[table_n]['join_fields'])))
        print("----------------")

        print("| FILTER FIELDS")
        filterFields_dict = {}
        for ff in table_info_dict["filter_fields"]:
            print(f"|--> {ff}")

            if "primaryProfession" in ff:
                df_res = data_df["primaryProfession"].apply(
                    lambda x: np.array(str(x).split(","), dtype='object')).values
                unique_vals = pd.Series(np.unique(np.concatenate(df_res)))
                ff_dict = analyze_filter_field_2(ff, data_df, unique_vals)
            else:
                ff_dict = analyze_filter_field(ff, data_df)

            print(f"|  {ff_dict}")

            filterFields_dict[ff] = ff_dict

        print("Scala:")
        print(toScala(str(filterFields_dict)))
        print("----------------")

        print("| GROUPBY FIELDS")
        groupby_fields = {}
        for gf in table_info_dict["groupby_fields"]:
            print(f"|--> {gf}")
            groupby_fields[gf] = analyze_groupby_field(gf, data_df) #"-1.0"

        print("Scala:")
        print(toScala(str(groupby_fields)))

        print("===============================================")


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description="Print the filter and group-by statistics of the IMDB (Kaggle) tables, as Scala maps "
                                             "for the table operator managers of the job generator.")
    ap.add_argument("data", help="folder with the preprocessed <table>.csv files")
    main(ap.parse_args().data)
