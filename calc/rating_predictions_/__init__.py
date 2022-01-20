import pandas as pd
from collections import defaultdict
from concurrent import futures
from functools import cache

pearson_sim = pd.DataFrame()
similarity_df = pd.DataFrame()
user_ratings = pd.DataFrame()
movieIDs = pd.DataFrame()
users = pd.DataFrame()
model_movies = dict()
K = 10

rating_dict = dict()
user_rating_sum = defaultdict(float)
user_rating_cnt = defaultdict(int)
movie_rating_sum = defaultdict(float)
movie_rating_cnt = defaultdict(int)
nearest_users = defaultdict(list)


@cache
def find_user_sim(u1, u2):
    return pearson_sim.loc[u1, u2]


def predict_user_rating(target_user, movie):
    rating = rating_dict.get((target_user, movie))
    rating_cnt = 0 if rating is None else 1
    rating = 0 if rating is None else rating

    if movie_rating_cnt[movie] - rating_cnt == 0:
        return None

    a1 = (movie_rating_sum[movie] - rating) / (movie_rating_cnt[movie] - rating_cnt)
    if K == 0:
        return a1

    a2 = 0
    a3 = 0
    cnt = 0
    for user in nearest_users[target_user]:
        r = rating_dict.get((user, movie))
        if r is None:
            continue

        if (user_rating_cnt[user] - 1) == 0:
            continue

        w = find_user_sim(user, target_user)

        rb = (user_rating_sum[user] - r) / (user_rating_cnt[user] - 1)

        a2 += (r - rb) * w
        a3 += abs(w)
        cnt += 1
        if cnt == K:
            break

    return a1 + a2 / a3 if a3 != 0 else a1


def predict_ratings_for_user(target_user):
    predicted_target_user_ratings = []
    # for movie in movieIDs:
    for movie in model_movies[target_user]:
        prediction = predict_user_rating(target_user, movie)
        predicted_target_user_ratings.append(
            {"user": target_user, "movie": movie, "prediction": prediction}
        )
    print(f"{target_user} predicted.")
    return predicted_target_user_ratings


def predict_all_user_ratings():
    with futures.ProcessPoolExecutor() as p:
        predicted_user_ratings = p.map(predict_ratings_for_user, users)
        predicted_user_ratings = [
            ratings for user in predicted_user_ratings for ratings in user
        ]
    return pd.DataFrame(predicted_user_ratings)


def predict_main(pearson_sim_, similarity_df_, user_ratings_, movie_ids_, model_movies_, users_, k_):
    global pearson_sim, similarity_df, user_ratings, movieIDs, model_movies, users, K
    model_movies = model_movies_
    pearson_sim = pearson_sim_.fillna(0)
    similarity_df = similarity_df_
    user_ratings = user_ratings_
    movieIDs = movie_ids_
    users = users_
    K = k_

    sim_sorted = similarity_df.sort_values(["u1", "sim"], ascending=[True, False])
    us = sim_sorted[['u1', 'u2']].values.tolist()
    for u1, u2 in us:
        nearest_users[u1].append(u2)

    rs = user_ratings.values.tolist()

    for user, movie, rating in rs:
        rating_dict[(user, movie)] = rating
        user_rating_sum[user] += rating
        user_rating_cnt[user] += 1
        movie_rating_sum[movie] += rating
        movie_rating_cnt[movie] += 1

    return predict_all_user_ratings()
