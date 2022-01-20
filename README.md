# README

> Python 版本 3.9.7

## 环境配置

（可选）创建`venv`虚拟环境（参考[文档](https://docs.python.org/zh-cn/3/library/venv.html)）：

```bash
cd movie-recommender
python3 -m venv ./venv
source ./venv/bin/activate # 激活虚拟环境，随平台不同
```

安装依赖：

```bash
pip3 install -r requirements.txt
```

## 运行

```bash
python3 main.py
```

## 参数调整

在`config.py`中调整`GlobalParameters`各变量的`default`值即可：

| 变量名称               | 取值                                   | 含义                                                         |
| ---------------------- | -------------------------------------- | ------------------------------------------------------------ |
| `top_n_movies`         | `int`                                  | 选取多少部最流行的电影并认为是所有观众的expected movies.     |
| `effective_threshold`  | `float`                                | 在选取每个用户最喜欢的导演/演员时，只考虑该用户打分至少为多少的电影。 |
| `similarity_threshold` | `float`                                | 在选取每个用户的active users时，只选取相似度至多为多少的用户。 |
| `method`               | `{'actor', 'director', 'transformer'}` | 为用户生成电影推荐列表的方式。                               |
| `nearest_k`            | `int`                                  | 在对用户对某部电影进行评分预测时，选用和该用户相似度最高的前多少位看过该电影的用户的评分。 |
| `rating_threshold`     | `float`                                | 将预测评分至少为多少的电影认为是serendipity movies。         |
| `max_list_size`        | `int`                                  | 为每个用户生成最大的推荐列表长度是多少。                     |
| `user_num`             | `int`                                  | 在对`unexpectedness`和`serenditity`进行估计时抽取多少用户。  |
| `root_path`            | 路径                                   | 输入和输出目录。                                             |
| `stat`                 | `{'precision', 'unexp-seren'}`         | 输出的统计量。                                               |

## 输入

将以下`.dat`文件（tsv文本文档）放在指定目录下：

|            | 路径                                    | 必要的列名                              |
| ---------- | --------------------------------------- | --------------------------------------- |
| 用户评分表 | `${root_path}/dat/user_ratedmovies.dat` | `userID movieID rating`                 |
| 电影信息表 | `${root_path}/dat/movies.dat`           | `id  title  year`                       |
| 电影类型表 | `${root_path}/dat/movie_genres.dat`     | `movieID genre`                         |
| 电影导演表 | `${root_path}/dat/movie_directors.dat`  | `movieID directorID directorName`       |
| 电影演员表 | `${root_path}/dat/movie_actors.dat`     | `movieID actorID    actorName  ranking` |

若用其他方法生成了为每位用户推荐的电影列表，将用户编号作为字典的键、对应的电影编号的列表作为字典的值，以`pickle`的格式写入`${root_path}/p/transformer_movies.p`，并将配置中的`method`调整为`transformer`。此时电影导演表和电影演员表可以为空。

## 输出

需要提前在`${root_path}`创建`csv`，`p`，`png`三个子文件夹。

`csv`：输出计算的统计量的`csv`表格。

`png`：输出统计图。

`p`：缓存计算过程中间量的`pickle`文件。若相关参数被修改，相关中间量会被重新计算。

## 开发

项目使用了`luigi`库进行工作流管理。可参考`luigi`的[文档](https://luigi.readthedocs.io/en/stable/tasks.html)。

项目将`Task`类重载为`MyTask`（见`helper/__init__.py`），来实现更方便的输入和输出。对于一个计算任务：

- 以类变量的形式引入该任务和该任务依赖的所有任务涉及到的参数；
- 在`requires`方法中返回依赖的所有任务实例的列表；
- 在`output`方法中返回一个`luigi.LocalTarget`实例。可以使用重载的`p_path`和`gen_path`方法自动生成路径名称；
- 在`run`方法中指定计算过程。可以使用重载的`input_`和`output_`方法实现更方便的`pickle`输入输出。

具体写法参考代码。