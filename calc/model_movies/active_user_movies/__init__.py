from helper import MyTask
import luigi
from calc.basic import UserRatings, Users
from calc.model_movies.active_user_movies.top_actor import TopActor
from calc.model_movies.active_user_movies.top_director import TopDirector
from calc.user_similarity import UserSimilarity
import numpy as np
from config import GlobalParameters


class ActiveUserMovies(MyTask):
    method = GlobalParameters.method
    similarity_threshold = GlobalParameters.similarity_threshold

    effective_threshold = GlobalParameters.effective_threshold

    def requires(self):
        return [
            TopActor(),
            TopDirector(),
            UserRatings(),
            Users(),
            UserSimilarity()
        ]

    def output(self):
        return luigi.LocalTarget(self.p_path("active_user_movies"))

    def run(self):
        (users_by_actors, actors_by_user), (users_by_directors, directors_by_user), \
            user_ratings, users, pearson_sim = self.input_()

        def find_user_similarity(u1, u2):
            return pearson_sim.loc[u1, u2]

        def find_active_user_movies(target_user):
            if self.method == 'director':
                users_by_stars, stars_by_user = users_by_directors, directors_by_user
            else:
                users_by_stars, stars_by_user = users_by_actors, actors_by_user

            relevant_stars = stars_by_user[target_user]
            active_users = list(
                filter(
                    lambda user: find_user_similarity(target_user, user) <= self.similarity_threshold,
                    users_by_stars[relevant_stars],
                )
            )

            active_user_ratings = user_ratings[user_ratings["userID"].isin(active_users)]
            active_user_ratings = active_user_ratings.groupby("movieID")['rating'].mean().sort_values(ascending=False)
            return np.array(active_user_ratings.index)

        def main():
            active_user_movies = {}
            for user in users:
                active_user_movies[user] = find_active_user_movies(user)
            return active_user_movies

        self.output_(main())
