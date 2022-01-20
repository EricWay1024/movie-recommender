import luigi
from helper import MyTask
from calc.rating_predictions_ import predict_main
from calc.basic import Users, UserRatings, Movies
from calc.user_similarity import SimilarityList, UserSimilarity
from calc.model_movies import ModelMovies
from config import GlobalParameters


class RatingPredictions(MyTask):
    nearest_k = GlobalParameters.nearest_k

    method = GlobalParameters.method
    similarity_threshold = GlobalParameters.similarity_threshold
    effective_threshold = GlobalParameters.effective_threshold

    def requires(self):
        return [UserSimilarity(), SimilarityList(), UserRatings(), Movies(),
                ModelMovies(),
                Users()]

    def output(self):
        return luigi.LocalTarget(self.p_path(f"rating_predictions_{self.nearest_k}"))

    def run(self):
        pearson_sim, similarity_df, user_ratings, movies, model_movies, users = self.input_()
        rating_predictions = predict_main(
            pearson_sim,
            similarity_df,
            user_ratings[['userID', 'movieID', 'rating']],
            movies['id'].unique(),
            model_movies,
            users,
            self.nearest_k
        )
        self.output_(rating_predictions)