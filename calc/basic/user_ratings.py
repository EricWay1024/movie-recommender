from helper import open_dat, DAT_PATHS, MyTask
import luigi


class UserRatings(MyTask):
    def output(self):
        return luigi.LocalTarget(self.p_path("user_ratings"))

    def run(self):
        res = open_dat(DAT_PATHS["user_ratings"])
        self.output_(res)
