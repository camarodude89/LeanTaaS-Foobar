import psycopg2
from psycopg2 import OperationalError
from ruamel.yaml import YAML
import keyring

yaml = YAML()
with open('config.yaml', 'r') as config_file:
    _config = yaml.load(config_file.read())
system = _config['connection']['system']


def create_postgres_connection():
    username = keyring.get_password(system, 'username')
    password = keyring.get_password(system, username)
    try:
        con = psycopg2.connect(database=_config['connection']['database'],
                               user=username, password=password)
        username, password = None, None
    except OperationalError as err:
        print(err)
        con = None
        sys.exit(1)
    return con


def get_movies_and_producers(con):
    with con:
        try:
            cur = con.cursor()
            cur.execute('SELECT movie, producer FROM movie_producer')
            results = cur.fetchall()
        except Exception as err:
            print(err)
            sys.exit(1)

    return results


def write_other_movie_counts(con, shared_producer_counts):
    with con:
        try:
            cur = con.cursor()
            query = 'INSERT INTO movie_problem_solution (movie, count) VALUES (%s, %s)'
            cur.executemany(query, shared_producer_counts)
            con.commit()
        except Exception as err:
            print(err)
            sys.exit(1)


if __name__ == '__main__':
    
    results = get_movies_and_producers(create_postgres_connection())
   
    # Create dictionary of producers and each producer's movies
    producer_movie_dict = {}
    movies = set()
    for pair in results:
        if pair[1] in producer_movie_dict:
            producer_movie_dict[pair[1]].append(pair[0])
        else:
            producer_movie_dict[pair[1]] = [pair[0]]
        movies.add(pair[0])

    # Create dictionary of movies and other movies sharing producers
    shared_producers_dict = {}
    for movie in movies:
        shared_producers_dict[movie] = set()
        for other_movies in producer_movie_dict.values():
            if movie in other_movies:
                index_to_exclude = other_movies.index(movie)
                shared_producers_dict[movie].update(other_movies[:index_to_exclude] + other_movies[index_to_exclude + 1:])

    # Create tuple of movies and the count of other movies sharing producers
    shared_producer_counts = tuple((movie, len(other_movies)) for movie, other_movies in shared_producers_dict.items())
    write_other_movie_counts(create_postgres_connection(), shared_producer_counts)
