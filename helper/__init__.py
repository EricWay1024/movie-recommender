import pickle
import luigi
import pandas as pd
import codecs
from config import GlobalParameters

ROOT_PATH = GlobalParameters().root_path

DAT_PATHS = {
    "user_ratings": f"{ROOT_PATH}/dat/user_ratedmovies.dat",
    "movies": f"{ROOT_PATH}/dat/movies.dat",
    "movie_genres": f"{ROOT_PATH}/dat/movie_genres.dat",
    "movie_directors": f"{ROOT_PATH}/dat/movie_directors.dat",
    "movie_actors": f"{ROOT_PATH}/dat/movie_actors.dat",
}


def open_dat(file_path) -> pd.DataFrame:
    return pd.read_csv(
        codecs.open(file_path, "r", encoding="utf-8", errors="ignore"), sep="\t"
    )


class MyTask(luigi.Task):
    def gen_path(self, name, file_type):
        param_str = '-'.join([""] + [str(getattr(self, x[0])) for x in self.get_params()])
        return f'{ROOT_PATH}/{file_type}/{name}{param_str}.{file_type}'

    def p_path(self, name):
        return self.gen_path(name, 'p')

    def output_(self, res):
        pickle.dump(res, open(self.output().path, 'wb'))

    def input_(self):
        inputs = self.input()
        return [pickle.load(open(i.path, 'rb')) for i in inputs]
