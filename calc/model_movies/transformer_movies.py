from helper import MyTask
import luigi


class TransformerMovies(MyTask):
    def output(self):
        return luigi.LocalTarget(self.p_path("transformer_movies"))

    def run(self):
        pass
