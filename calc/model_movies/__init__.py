from helper import MyTask
import luigi
from calc.model_movies.active_user_movies import ActiveUserMovies
from calc.model_movies.transformer_movies import TransformerMovies
from config import GlobalParameters


class ModelMovies(MyTask):
    method = GlobalParameters.method

    similarity_threshold = GlobalParameters.similarity_threshold
    effective_threshold = GlobalParameters.effective_threshold

    def requires(self):
        if self.method in ['director', 'actor']:
            return [ActiveUserMovies()]
        else:
            return [TransformerMovies()]

    def output(self):
        return luigi.LocalTarget(self.p_path(f'model_movies'))

    def run(self):
        model_movies, = self.input_()
        self.output_(model_movies)
