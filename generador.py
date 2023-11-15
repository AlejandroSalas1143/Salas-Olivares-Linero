import getopt
import os
import argparse
import sys
import bz2
import json
import networkx as nx
import time
from collections import defaultdict
from itertools import combinations
import glob
from pathlib import Path

def parse_args():
    parser = argparse.ArgumentParser(description="Descripción de tu script.")
    parser.add_argument("-d", "--directory", required=True, help="Directorio de entrada")
    parser.add_argument("--fi", "--fecha_inicial", help="Fecha inicial en formato dd-mm-aa")
    parser.add_argument("--ff", "--fecha_final", help="Fecha final en formato dd-mm-aa")
    parser.add_argument("-ht", "--hashtags_file", help="Nombre de archivo de texto con hashtags")
    args = parser.parse_args()
    return args.directory, args.fi, args.ff, args.hashtags_file

def get_tweet_id(tweet):
    return tweet['id_str'] if 'retweeted_status' in tweet else str(tweet['id'])

def process_original_tweet(tweet, retweets_info, mentions_info, hashtags_set=None):
    if 'user' in tweet:
        tweet_author_username = tweet['user']['screen_name']
        tweet_id = get_tweet_id(tweet)

        # Verificar si hay hashtags y filtrar por ellos
        if 'entities' in tweet and 'hashtags' in tweet['entities']:
            tweet_hashtags = {tag['text'].lower() for tag in tweet['entities']['hashtags']}
            if hashtags_set and not tweet_hashtags.intersection(hashtags_set):
                return

        retweets_info.setdefault(tweet_author_username, {"tweets": {}})
        retweets_info[tweet_author_username]["tweets"].setdefault(tweet_id, {"retweetedBy": []})

        process_mentions(tweet, mentions_info)

def process_retweet(tweet, retweets_info, mentions_info, hashtags_set=None):
    if 'retweeted_status' in tweet and 'user' in tweet['retweeted_status']:
        retweet_author_username = tweet['retweeted_status']['user']['screen_name']
        retweeted_tweet_id = get_tweet_id(tweet['retweeted_status'])

        # Verificar si hay hashtags y filtrar por ellos
        if 'entities' in tweet['retweeted_status'] and 'hashtags' in tweet['retweeted_status']['entities']:
            retweet_hashtags = {tag['text'].lower() for tag in tweet['retweeted_status']['entities']['hashtags']}
            if hashtags_set and not retweet_hashtags.intersection(hashtags_set):
                return

        retweets_info.setdefault(retweet_author_username, {"tweets": {}})
        retweets_info[retweet_author_username]["tweets"].setdefault(retweeted_tweet_id, {"retweetedBy": []})
        retweets_info[retweet_author_username]["tweets"][retweeted_tweet_id]["retweetedBy"].append(tweet['user']['screen_name'])

        original_tweet = tweet['retweeted_status']
        process_mentions(original_tweet, mentions_info)

def process_mentions(tweet, mentions_info):
    if 'entities' in tweet and 'user_mentions' in tweet['entities'] and tweet['entities']['user_mentions']:
        mentioned_usernames = set(mention['screen_name'] for mention in tweet['entities']['user_mentions'])
        for mentioned_username in mentioned_usernames:
            mentions_info.setdefault(mentioned_username, {"mentions": []})
            mentions_info[mentioned_username]["mentions"].append({"mentionBy": tweet['user']['screen_name'], "tweets": [get_tweet_id(tweet)]})

def decompress_and_create_json_files(directory, fecha_inicial=None, fecha_final=None, hashtags_file=None):
    retweets_info = {}
    mentions_info = {}
    json_files = [] 
    # Cargar hashtags desde el archivo
    hashtags_set = set()
    if hashtags_file:
        with open(hashtags_file, 'r') as hashtags_file:
            hashtags_set = {line.strip().lower() for line in hashtags_file}
    #FILE_PATHH =[]
    # Utilizamos Path para manejar rutas de manera más eficiente
    base_path = Path(directory)

    if fecha_inicial!=None:
        fecha_iniciall = convert_year_to_4_digits(fecha_inicial[-2:])
        directory_path_i = os.path.join(fecha_iniciall, fecha_inicial[3:5], fecha_inicial[0:2])
        directory_path_i = directory_path_i.replace("\\", "/")
    
    if fecha_final!=None:
        fecha_finall = convert_year_to_4_digits(fecha_final[-2:])
        directory_path_f = os.path.join(fecha_finall, fecha_final[3:5], fecha_final[0:2])
        directory_path_f = directory_path_f.replace("\\", "/")

    file_paths = base_path.rglob('*.json.bz2')

    file_paths_list = list(file_paths)

    if fecha_inicial==None and fecha_final==None:
        file_paths_generator=iter(file_paths)
    else:
        if fecha_inicial!=None and fecha_final==None:
            for i, ruta in enumerate(file_paths_list):
                A= str(ruta).replace("\\", "/")
                if directory_path_i in str(A):
                    # La cadena de texto ha sido encontrada, recortar la lista desde este punto
                    FILE_PATHH= file_paths_list[i:]
                    break
            file_paths_generator = iter(FILE_PATHH)
        else:
            if fecha_final!=None and fecha_inicial==None:
                for i, ruta in reversed(list(enumerate(file_paths_list))):
                    A= str(ruta).replace("\\", "/")
                    if directory_path_f in str(A):
                    # La cadena de texto ha sido encontrada, recortar la lista desde este punto
                        FILE_PATHHf= file_paths_list[:i]
                        break
                file_paths_generator = iter(FILE_PATHHf)
            else:
                for i, ruta in enumerate(file_paths_list):
                        A= str(ruta).replace("\\", "/")
                        if directory_path_i in str(A):
                            # La cadena de texto ha sido encontrada, recortar la lista desde este punto
                            FILE_PATHH= file_paths_list[i:]
                            break
                file_paths_generator = iter(FILE_PATHH)
                for i, ruta in reversed(list(enumerate(FILE_PATHH))):
                    A= str(ruta).replace("\\", "/")
                    if directory_path_f in str(A):
                        # La cadena de texto ha sido encontrada, recortar la lista desde este punto
                        FILE_PATHHf= FILE_PATHH[:i]
                        break
                file_paths_generator = iter(FILE_PATHHf)

    
    for file_path in file_paths_generator:
        json_file_path = file_path.with_suffix('.json')
        json_files.append(json_file_path)

        with bz2.BZ2File(file_path, 'rb') as source, open(json_file_path, 'wb') as target:
            target.write(source.read())

        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            for line in json_file:
                tweet = json.loads(line)
                if 'retweeted_status' in tweet:
                    process_retweet(tweet, retweets_info, mentions_info, hashtags_set)
                else:
                    process_original_tweet(tweet, retweets_info, mentions_info, hashtags_set)

    return retweets_info, mentions_info, json_files

def decompress_and_create_json_files_directory(directory, hashtags_file=None):
    retweets_info = {}
    mentions_info = {}
    json_files = []
    hashtags_set = set()
    if hashtags_file:
        with open(hashtags_file, 'r') as hashtags_file:
            hashtags_set = {line.strip().lower() for line in hashtags_file}

    # Utilizamos Path para manejar rutas de manera más eficiente
    base_path = Path(directory)

    # Utilizamos rglob para buscar recursivamente archivos *.json.bz2
    file_paths = base_path.rglob('*.json.bz2')

    for file_path in file_paths:
        json_file_path = file_path.with_suffix('.json')
        json_files.append(json_file_path)

        with bz2.BZ2File(file_path, 'rb') as source, open(json_file_path, 'wb') as target:
            target.write(source.read())


        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            for line in json_file:
                tweet = json.loads(line)
                if 'retweeted_status' in tweet:
                    process_retweet(tweet, retweets_info, mentions_info, hashtags_set)
                else:
                    process_original_tweet(tweet, retweets_info, mentions_info, hashtags_set)

    return retweets_info, mentions_info,json_files

def convert_year_to_4_digits(year):
    if len(year) == 2:
        return "20" + year

def generate_retweets_json(retweets_info):
    retweets_json = {"retweets": []}

    for author, author_info in retweets_info.items():
        total_retweets = sum(len(tweet_info["retweetedBy"]) for tweet_info in author_info["tweets"].values())
        
        # Agregar la condición para incluir solo autores con al menos un retweet
        if total_retweets > 0:
            author_data = {"username": author, "receivedRetweets": total_retweets, "tweets": {}}

            for tweet_id, tweet_info in author_info["tweets"].items():
                retweeted_by = tweet_info["retweetedBy"]
                tweet_data = {"retweetedBy": retweeted_by}
                author_data["tweets"]["tweetId: {}".format(tweet_id)] = tweet_data

            retweets_json["retweets"].append(author_data)

    # Ordenar el JSON por número total de retweets al usuario (de mayor a menor)
    retweets_json["retweets"] = sorted(retweets_json["retweets"], key=lambda x: x["receivedRetweets"], reverse=True)

    with open("rt.json", "w", encoding="utf-8") as json_file:
        json.dump(retweets_json, json_file, ensure_ascii=False, indent=2)

    return retweets_json

def generate_mentions_json(mentions_info):
    mentions_json = {"mentions": []}
    #pprint(mentions_info)

    for username, user_info in mentions_info.items():
        total_mentions = sum(len(mention_info['tweets']) for mention_info in user_info['mentions'])
        user_data = {"username": username, "receivedMentions": total_mentions, "mentions": []}

        for mention_info in user_info["mentions"]:
            mention_data = {"mentionBy": mention_info["mentionBy"], "tweets": mention_info["tweets"]}
            user_data["mentions"].append(mention_data)

        mentions_json["mentions"].append(user_data)

    # Ordenar el JSON por número total de menciones al usuario (de mayor a menor)
    mentions_json["mentions"] = sorted(mentions_json["mentions"], key=lambda x: x["receivedMentions"], reverse=True)

    with open("mención.json", "w", encoding="utf-8") as json_file:
        json.dump(mentions_json, json_file, ensure_ascii=False, indent=2)

    return mentions_json

def generate_retweets_graph(retweets_json):
    G = nx.Graph()

    for author_data in retweets_json["retweets"]:
        author = author_data["username"]
        
        for tweet_id, tweet_data in author_data["tweets"].items():
            G.add_node(author)
            for retweeted_by in tweet_data["retweetedBy"]:
                G.add_node(retweeted_by)
                G.add_edge(author, retweeted_by)

            # Conectar al autor con todos los que retuitearon ese tweet
            G.add_edges_from([(author, retweeted_by) for retweeted_by in tweet_data["retweetedBy"]])

    nx.write_gexf(G, "rt.gexf")


def generate_mentions_graph(mentions_json):
    G = nx.Graph()

    for user_data in mentions_json["mentions"]:
        username = user_data["username"]
        
        for mention_data in user_data["mentions"]:
            mention_by = mention_data["mentionBy"]
            G.add_node(username)
            G.add_node(mention_by)
            G.add_edge(username, mention_by)

    nx.write_gexf(G, "mención.gexf")


def generate_corrtweets_json(retweets_info):
    corrtweets_dict = defaultdict(set)
    # Recopilar información sobre quién retuiteó a cada autor
    for author, author_info in retweets_info.items():
        for tweet_info in author_info["tweets"].values():
            retweeted_by = tweet_info["retweetedBy"]
        
        # Verificar si tweet_info["retweetedBy"] no está vacío antes de agregar a corrtweets_dict
            if retweeted_by:
                retweeters = set(retweeted_by)
                corrtweets_dict[author].update(retweeters)

    #print(corrtweets_dict)

    # Encontrar corretweets comparando retweeters entre autores
    corrtweets_list = []
    for author1, author2 in combinations(corrtweets_dict, 2):
        common_retweeters = corrtweets_dict[author1] & corrtweets_dict[author2]
        if common_retweeters:
            coretweet_data = {
                'authors': {'u1': author1, 'u2': author2},
                'totalCoretweets': len(common_retweeters),
                'retweeters': list(common_retweeters)
            }
            corrtweets_list.append(coretweet_data)
    
    corrtweets_list = sorted(corrtweets_list, key=lambda x: x['totalCoretweets'], reverse=True)

    corrtweets_json = {'coretweets': corrtweets_list}
    
    with open('corrtw.json', 'w', encoding='utf-8') as json_file:
        json.dump(corrtweets_json, json_file, ensure_ascii=False, indent=2)


    return corrtweets_json



def generate_corrtweets_graph(corrtweets_info):
    G = nx.Graph()

    for corrtweet_info in corrtweets_info["coretweets"]:
        author1 = corrtweet_info["authors"]["u1"]
        author2 = corrtweet_info["authors"]["u2"]
        total_corretweets = corrtweet_info["totalCoretweets"]

        # Agregar nodos y aristas al grafo
        G.add_node(author1)
        G.add_node(author2)
        G.add_edge(author1, author2, weight=total_corretweets)

    nx.write_gexf(G, "corrtw.gexf")


if __name__ == "__main__":
    start_time = time.time()
    directory, fecha_inicial, fecha_final, hashtags_file = parse_args()
    if fecha_inicial==None and fecha_final==None:
         retweets_info, mentions_info, json_files = decompress_and_create_json_files_directory(directory, hashtags_file)
    else:
         retweets_info, mentions_info, json_files = decompress_and_create_json_files(directory, fecha_inicial, fecha_final, hashtags_file)

    tweets=retweets_info
    rt_json= generate_retweets_json(retweets_info)
    generate_retweets_graph(rt_json)
    mentions_json= generate_mentions_json(mentions_info)
    generate_mentions_graph(mentions_json)
    corrtweets_json = generate_corrtweets_json(retweets_info)
    generate_corrtweets_graph(corrtweets_json)

    for json_file in json_files:
        os.remove(json_file.with_suffix('.json'))
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Proceso completado. Tiempo total de ejecución: {total_time} segundos.")
