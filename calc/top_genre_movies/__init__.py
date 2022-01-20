from helper import MyTask
import luigi
from calc.basic import MovieGenres, Users, UserRatings
# from calc.movie_series import Us
from calc.basic.top_movies import TopMovies
import pandas as pd


class TopGenreMovies(MyTask):
    def requires(self):
        return [MovieGenres(), UserRatings(), Users()]

    def output(self):
        return luigi.LocalTarget(self.p_path("top_genre_movies"))

    def run(self):
        movie_genres, user_ratings, users = self.input_()

        def find_top_genre_movies(target_user):
            """
            Find all movies of the genre that the target user rates the most.
            """
            target_user_ratings = user_ratings[user_ratings["userID"] == target_user]
            target_user_genres = pd.merge(
                target_user_ratings, movie_genres, "left", ["movieID", "movieID"]
            )
            target_user_genre_rank = (
                target_user_genres.groupby(["genre"]).mean().sort_values(by='rating', ascending=False)
            )
            top_genre = target_user_genre_rank.index[0]
            return movie_genres[movie_genres["genre"] == top_genre]["movieID"]

        def main():
            top_genre_movies = {}
            for user in users:
                top_genre_movies[user] = find_top_genre_movies(user)
            return top_genre_movies

        self.output_(main())
