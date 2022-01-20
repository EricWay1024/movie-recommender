import luigi


class GlobalParameters(luigi.Config):
    top_n_movies = luigi.IntParameter(default=200)
    effective_threshold = luigi.FloatParameter(default=4.0)
    similarity_threshold = luigi.FloatParameter(default=0.3)
    method = luigi.Parameter(default='actor')
    nearest_k = luigi.IntParameter(default=10)
    rating_threshold = luigi.FloatParameter(default=3.0)
    max_list_size = luigi.IntParameter(default=500)
    user_num = luigi.IntParameter(default=500)
    root_path = luigi.Parameter(default='data/1')
    stat = luigi.Parameter(default='precision')  # 'unexp-seren' or 'precision'
