from helper import MyTask
from calc.basic import Users, UserRatings
import luigi
from collections import defaultdict
import pandas as pd


class UserSimilarity(MyTask):
    def requires(self):
        return [UserRatings()]

    def output(self):
        return luigi.LocalTarget(self.p_path("user_similarity"))

    def run(self):
        """
        Find the Pearson similarity between any two users, represented by a n*n
        matrix cor. THe similarity between user u and v is cor.loc[u, v].
        """
        user_ratings, = self.input_()
        ratings_ = defaultdict(dict)
        rating_list = user_ratings[["userID", "movieID", "rating"]].values.tolist()
        for user, movie, rating in rating_list:
            ratings_[movie][user] = rating
        ratings_ = list(ratings_.values())
        df = pd.DataFrame(ratings_)
        cor = df.corr()
        self.output_(cor)


class SimilarityList(MyTask):
    def requires(self):
        return [UserSimilarity()]

    def output(self):
        return luigi.LocalTarget(self.p_path("similarity_list"))

    def run(self):
        pearson_sim, = self.input_()

        res = pearson_sim.stack().reset_index().rename(columns={
            'level_0': 'u1',
            'level_1': 'u2',
            0: 'sim'
        }).query('u1 != u2')

        self.output_(res)
