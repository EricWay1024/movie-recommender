import pandas as pd

from helper import MyTask
import luigi
from calc.model_movies import ModelMovies
from calc.top_genre_movies import TopGenreMovies
from calc.expected_movies import ExpectedMovies
from calc.rating_predictions import RatingPredictions
from calc.basic import Users
from calc.stats_ import calc_stats_main, calc_precision_main
import matplotlib.pyplot as plt
from config import GlobalParameters


class Stats(MyTask):
    rating_threshold = GlobalParameters.rating_threshold
    max_list_size = GlobalParameters.max_list_size
    user_num = GlobalParameters.user_num
    stat = GlobalParameters.stat

    method = GlobalParameters.method
    similarity_threshold = GlobalParameters.similarity_threshold
    effective_threshold = GlobalParameters.effective_threshold
    n = GlobalParameters.top_n_movies
    nearest_k = GlobalParameters.nearest_k

    def requires(self):
        return [
            ModelMovies(),
            TopGenreMovies(),
            ExpectedMovies(),
            RatingPredictions(),
            Users()
        ]

    def output(self):
        return luigi.LocalTarget(self.p_path(f"stats"))

    def run(self):
        active_user_movies, top_genre_movies, expected_movies, rating_predictions, users = self.input_()
        calc = calc_stats_main if self.stat == 'unexp-seren' else calc_precision_main
        res_ = calc(
            active_user_movies,
            top_genre_movies,
            expected_movies,
            rating_predictions,
            users,
            self.rating_threshold,
            self.user_num,
            self.max_list_size,
        )
        res_df = []
        if self.stat == 'unexp-seren':
            for n, unexp, seren in res_:
                res_df.append({
                    'n': n,
                    'unexp': unexp,
                    'seren': seren
                })
        else:
            for n, precision in res_:
                res_df.append({
                    'n': n,
                    'precision': precision
                })
        res_df = pd.DataFrame(res_df)
        res_df.set_index(["n"]).to_csv(self.gen_path('stats', 'csv'))
        self.output_(res_)


class PlotFig(MyTask):
    rating_threshold = GlobalParameters.rating_threshold
    max_list_size = GlobalParameters.max_list_size
    user_num = GlobalParameters.user_num
    stat = GlobalParameters.stat

    method = GlobalParameters.method
    similarity_threshold = GlobalParameters.similarity_threshold
    effective_threshold = GlobalParameters.effective_threshold
    n = GlobalParameters.top_n_movies
    nearest_k = GlobalParameters.nearest_k

    def requires(self):
        return [Stats()]

    def output(self):
        return luigi.LocalTarget(self.gen_path('fig', 'png'))

    def run(self):
        res, = self.input_()

        if self.stat == 'unexp-seren':
            plt.clf()
            plt.title("Unexpectedness & Serendipity")
            plt.plot([x[0] for x in res], [x[1] for x in res], label="UNEXP")
            plt.plot([x[0] for x in res], [x[2] for x in res], label="SEREN")
            plt.ylim(0, 1)
            plt.xlim(0, self.max_list_size)
            plt.xlabel("Recommendation list size")
            plt.ylabel("Unexpectedness & Serendipity")
            plt.legend(loc="best")
            plt.savefig(self.output().path)
        else:
            plt.clf()
            plt.title("Precision")
            plt.plot([x[0] for x in res], [x[1] for x in res], label="Precision")
            plt.ylim(0, 1)
            plt.xlim(0, self.max_list_size)
            plt.xlabel("Recommendation list size")
            plt.ylabel("Precision")
            plt.legend(loc="best")
            plt.savefig(self.output().path)
