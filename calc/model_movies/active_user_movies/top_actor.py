from helper import MyTask
import luigi
from calc.basic import MovieActors, UserRatings, Users
import pandas as pd
from collections import defaultdict
from config import GlobalParameters


class TopActor(MyTask):
    effective_threshold = GlobalParameters.effective_threshold

    def requires(self):
        return [MovieActors(), UserRatings(), Users()]

    def output(self):
        return luigi.LocalTarget(self.p_path("top_actor"))

    def run(self):
        movie_actors, user_ratings, users = self.input_()

        user_ratings = pd.merge(
            user_ratings[['userID', 'movieID', 'rating']],
            movie_actors.query('ranking == 1')[['movieID', 'actorName']],
            how='inner',
            left_on=['movieID'],
            right_on=['movieID'],
        )

        users_by_actors = defaultdict(list)
        actors_by_user = {}

        def find_top_rated_actors(target_user: int) -> frozenset:
            """Find the set of top rated directors of the target user."""

            effective_ratings = user_ratings[
                (user_ratings["userID"] == target_user)
                & (user_ratings["rating"] >= self.effective_threshold)
                ]

            if len(effective_ratings) == 0:
                return frozenset([])

            merged_ratings = pd.DataFrame(effective_ratings.groupby(["actorName"]).size(), columns=['size'])
            merged_ratings['sum'] = effective_ratings.groupby(["actorName"]).sum()['rating']
            merged_ratings = merged_ratings.sort_values(by=['size', 'sum'], ascending=[False, False])
            return merged_ratings.index[0]

        for user in users:
            relevant_directors = find_top_rated_actors(user)
            users_by_actors[relevant_directors].append(user)
            actors_by_user[user] = relevant_directors

        self.output_((users_by_actors, actors_by_user))
