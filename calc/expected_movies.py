from helper import MyTask
import luigi
from calc.basic import UserRatings, Users
from calc.movie_series import MovieSeries
from calc.basic.top_movies import TopMovies
import numpy as np
from config import GlobalParameters


class ExpectedMovies(MyTask):
    n = GlobalParameters.top_n_movies

    def requires(self):
        return [UserRatings(), MovieSeries(), TopMovies(), Users()]

    def output(self):
        return luigi.LocalTarget(self.p_path("expected_movies"))

    def run(self):
        user_ratings, movies_in_series, top_movies, users = self.input_()

        def find_expected_movies(target_user: int) -> np.ndarray:
            """Find all expected movies of the target user, i.e. all rated movies,
            all movies which share a movie series with any of the rated movies, and
            all top 200 movies."""

            target_user_ratings = user_ratings[user_ratings["userID"] == target_user]
            rated_movies = target_user_ratings["movieID"].unique()
            expected_movies = set(rated_movies)
            for movie in rated_movies:
                expected_movies.update(movies_in_series[movie])
            expected_movies.update(top_movies["id"])
            return np.array(list(expected_movies))

        def main():
            expected_movies = {}
            for user in users:
                expected_movies[user] = find_expected_movies(user)
            return expected_movies

        self.output_(main())
