"""
Schemas of the 21 JOB (Join Order Benchmark) tables of the IMDB dump, used by MetaDataAnalyzer.py and by the
SQL Server loader (datafarm/execution/sql/load_sqlserver.py).

Filter predicates, selectivities, distinct counts and row counts are in
datafarm/instantiation/src/main/resources/job/job_catalog.json, which the job generator reads.
"""

# Columns in file order, as in datafarm/instantiation/.../dataset/job/JOBSchema.scala (keep both in sync).
# (name, is_int, nullable)
I, T = True, False
SCHEMA = {
    "aka_name": [("id", I, 0), ("person_id", I, 0), ("name", T, 0), ("imdb_index", T, 1), ("name_pcode_cf", T, 1),
                 ("name_pcode_nf", T, 1), ("surname_pcode", T, 1), ("md5sum", T, 1)],
    "aka_title": [("id", I, 0), ("movie_id", I, 0), ("title", T, 0), ("imdb_index", T, 1), ("kind_id", I, 0),
                  ("production_year", I, 1), ("phonetic_code", T, 1), ("episode_of_id", I, 1), ("season_nr", I, 1),
                  ("episode_nr", I, 1), ("note", T, 1), ("md5sum", T, 1)],
    "cast_info": [("id", I, 0), ("person_id", I, 0), ("movie_id", I, 0), ("person_role_id", I, 1), ("note", T, 1),
                  ("nr_order", I, 1), ("role_id", I, 0)],
    "char_name": [("id", I, 0), ("name", T, 0), ("imdb_index", T, 1), ("imdb_id", I, 1), ("name_pcode_nf", T, 1),
                  ("surname_pcode", T, 1), ("md5sum", T, 1)],
    "comp_cast_type": [("id", I, 0), ("kind", T, 0)],
    "company_name": [("id", I, 0), ("name", T, 0), ("country_code", T, 1), ("imdb_id", I, 1), ("name_pcode_nf", T, 1),
                     ("name_pcode_sf", T, 1), ("md5sum", T, 1)],
    "company_type": [("id", I, 0), ("kind", T, 0)],
    "complete_cast": [("id", I, 0), ("movie_id", I, 1), ("subject_id", I, 0), ("status_id", I, 0)],
    "info_type": [("id", I, 0), ("info", T, 0)],
    "keyword": [("id", I, 0), ("keyword", T, 0), ("phonetic_code", T, 1)],
    "kind_type": [("id", I, 0), ("kind", T, 0)],
    "link_type": [("id", I, 0), ("link", T, 0)],
    "movie_companies": [("id", I, 0), ("movie_id", I, 0), ("company_id", I, 0), ("company_type_id", I, 0),
                        ("note", T, 1)],
    "movie_info": [("id", I, 0), ("movie_id", I, 0), ("info_type_id", I, 0), ("info", T, 0), ("note", T, 1)],
    "movie_info_idx": [("id", I, 0), ("movie_id", I, 0), ("info_type_id", I, 0), ("info", T, 0), ("note", T, 1)],
    "movie_keyword": [("id", I, 0), ("movie_id", I, 0), ("keyword_id", I, 0)],
    "movie_link": [("id", I, 0), ("movie_id", I, 0), ("linked_movie_id", I, 0), ("link_type_id", I, 0)],
    "name": [("id", I, 0), ("name", T, 0), ("imdb_index", T, 1), ("imdb_id", I, 1), ("gender", T, 1),
             ("name_pcode_cf", T, 1), ("name_pcode_nf", T, 1), ("surname_pcode", T, 1), ("md5sum", T, 1)],
    "person_info": [("id", I, 0), ("person_id", I, 0), ("info_type_id", I, 0), ("info", T, 0), ("note", T, 1)],
    "role_type": [("id", I, 0), ("role", T, 0)],
    "title": [("id", I, 0), ("title", T, 0), ("imdb_index", T, 1), ("kind_id", I, 0), ("production_year", I, 1),
              ("imdb_id", I, 1), ("phonetic_code", T, 1), ("episode_of_id", I, 1), ("season_nr", I, 1),
              ("episode_nr", I, 1), ("series_years", T, 1), ("md5sum", T, 1)],
}
