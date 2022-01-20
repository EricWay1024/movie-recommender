import luigi
from helper import MyTask
from calc.basic import UserRatings, Movies
from config import GlobalParameters


class TopMovies(MyTask):
    n = GlobalParameters.top_n_movies

    def requires(self):
        return [UserRatings(), Movies()]

    def output(self):
        return luigi.LocalTarget(self.p_path("top_movies"))

    def run(self):
        user_ratings, movies = self.input_()
        top_movie_ids = (
            user_ratings.groupby("movieID").size().sort_values(ascending=False)[:self.n]
        )
        top_movies = movies[movies["id"].isin(top_movie_ids.index)]
        self.output_(top_movies)
