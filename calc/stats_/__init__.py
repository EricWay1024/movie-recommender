import numpy as np
import pandas as pd
from concurrent import futures

active_user_movies_dict = dict()
top_genre_movies_dict = dict()
expected_movies_dict = dict()
rating_predictions = pd.DataFrame()
users = np.array([])
RATING_THRESHOLD = 3.0
USER_NUM = 500
max_list_size = 500


def calc_stats(target_user, n):
    """
    Calculate serendipity for target_user, when recommendation
    list size is n.
    """
    active_user_movies = active_user_movies_dict[target_user]
    if len(active_user_movies) < n:
        return None
    active_user_movies = np.array(active_user_movies[:n])

    expected_movies = expected_movies_dict[target_user]
    pm_movies = top_genre_movies_dict[target_user]
    try:
        pm_movies = pm_movies.sample(n, replace=False)
    except ValueError:
        pm_movies = pm_movies.sample(n, replace=True)
    unexpected_movies = active_user_movies[
        (~np.isin(active_user_movies, expected_movies))
        & (~np.isin(active_user_movies, pm_movies))
    ]

    user_predictions = rating_predictions[rating_predictions["user"] == target_user]
    if len(user_predictions) == 0:
        return None

    serendipity_movies = user_predictions[
        (user_predictions["movie"].isin(unexpected_movies))
        & (user_predictions["prediction"] >= RATING_THRESHOLD)
    ]

    unexp = len(unexpected_movies) / len(active_user_movies)
    seren = len(serendipity_movies) / len(active_user_movies)
    return unexp, seren


def calc_precision(target_user, n):
    active_user_movies = active_user_movies_dict[target_user]
    if len(active_user_movies) < n:
        return None
    active_user_movies = np.array(active_user_movies[:n])
    expected_movies = expected_movies_dict[target_user]
    hit_movies = active_user_movies[np.isin(active_user_movies, expected_movies)]
    precision = len(hit_movies) / len(active_user_movies)
    return precision


def calc_precision_for_list_size(n):
    stats_list = []

    for user in users:
        stat = calc_precision(user, n)
        if stat is None:
            continue
        stats_list.append({
            'user': user,
            'precision': stat,
        })

    df = pd.DataFrame(stats_list)
    res = n, df['precision'].mean()
    print(res)
    return res


def calc_stats_for_list_size(n):
    """
    Calculate the mean unexpectedness for user_num users when
    recommendation list size is n.
    """
    user_sample = np.random.choice(users, USER_NUM * 2)
    stats_list = []
    for user in user_sample:
        stats = calc_stats(user, n)
        if stats is None:
            continue
        unexp, seren = stats
        stats_list.append({
            'user': user,
            'unexp': unexp,
            'seren': seren,
        })
        if len(stats_list) == USER_NUM:
            break

    df = pd.DataFrame(stats_list)
    res = n, df['unexp'].mean(), df['seren'].mean()
    print(res)
    return res


def calc_stats_main(
        active_user_movies_dict_,
        top_genre_movies_dict_,
        expected_movies_dict_,
        rating_predictions_,
        users_,
        rating_threshold_=3.0,
        user_num_=500,
        max_list_size_=500
):
    global active_user_movies_dict, top_genre_movies_dict
    global expected_movies_dict, rating_predictions, users
    global RATING_THRESHOLD, USER_NUM, max_list_size

    max_list_size = max_list_size_
    active_user_movies_dict = active_user_movies_dict_
    top_genre_movies_dict = top_genre_movies_dict_
    expected_movies_dict = expected_movies_dict_
    rating_predictions = rating_predictions_
    users = users_
    RATING_THRESHOLD = rating_threshold_
    USER_NUM = user_num_

    with futures.ProcessPoolExecutor() as p:
        res = p.map(calc_stats_for_list_size, range(1, max_list_size + 1))

    return list(res)


def calc_precision_main(
        active_user_movies_dict_,
        top_genre_movies_dict_,
        expected_movies_dict_,
        rating_predictions_,
        users_,
        rating_threshold_=3.0,
        user_num_=500,
        max_list_size_=500
):
    global active_user_movies_dict, top_genre_movies_dict
    global expected_movies_dict, rating_predictions, users
    global RATING_THRESHOLD, USER_NUM, max_list_size

    max_list_size = max_list_size_
    active_user_movies_dict = active_user_movies_dict_
    top_genre_movies_dict = top_genre_movies_dict_
    expected_movies_dict = expected_movies_dict_
    rating_predictions = rating_predictions_
    users = users_
    RATING_THRESHOLD = rating_threshold_
    USER_NUM = user_num_

    with futures.ProcessPoolExecutor() as p:
        res = p.map(calc_precision_for_list_size, range(1, max_list_size + 1))

    return list(res)