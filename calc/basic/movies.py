from helper import open_dat, DAT_PATHS, MyTask
import luigi


class Movies(MyTask):
    def output(self):
        return luigi.LocalTarget(self.p_path("movies"))

    def run(self):
        r = open_dat(DAT_PATHS["movies"])
        self.output_(r)


class MovieGenres(MyTask):
    def output(self):
        return luigi.LocalTarget(self.p_path("movie_genres"))

    def run(self):
        self.output_(open_dat(DAT_PATHS["movie_genres"]))


class MovieDirectors(MyTask):
    def output(self):
        return luigi.LocalTarget(self.p_path("movie_directors"))

    def run(self):
        self.output_(open_dat(DAT_PATHS["movie_directors"]))


class MovieActors(MyTask):
    def output(self):
        return luigi.LocalTarget(self.p_path("movie_actors"))

    def run(self):
        self.output_(open_dat(DAT_PATHS["movie_actors"]))
