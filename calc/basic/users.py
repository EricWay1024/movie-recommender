from helper import MyTask
import luigi
from calc.basic.user_ratings import UserRatings


class Users(MyTask):
    def requires(self):
        return [UserRatings()]

    def output(self):
        return luigi.LocalTarget(self.p_path("users"))

    def run(self):
        user_ratings, = self.input_()
        users = user_ratings['userID'].unique()
        self.output_(users)
