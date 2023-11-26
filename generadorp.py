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
from datetime import datetime
import shutil
from mpi4py import MPI


def parse_args():
    parser = argparse.ArgumentParser(description="Descripción de tu script.", add_help=False)
    parser.add_argument("-d", "--directory", required=True, help="Directorio de entrada")
    parser.add_argument("-fi", "--fecha_inicial", help="Fecha inicial en formato dd-mm-aa")
    parser.add_argument("-ff", "--fecha_final", help="Fecha final en formato dd-mm-aa")
    parser.add_argument("-h", "--hashtags_file", help="Nombre de archivo de texto con hashtags")
    
    # New options for graph and JSON generation
    parser.add_argument("-grt", "--generate_retweet_graph", action="store_true", help="Generar grafo de retweets")
    parser.add_argument("-jrt", "--generate_retweet_json", action="store_true", help="Generar JSON de retweets")
    parser.add_argument("-gm", "--generate_mentions_graph", action="store_true", help="Generar grafo de menciones")
    parser.add_argument("-jm", "--generate_mentions_json", action="store_true", help="Generar JSON de menciones")
    parser.add_argument("-gcrt", "--generate_corretweet_graph", action="store_true", help="Generar grafo de corretweets")
    parser.add_argument("-jcrt", "--generate_corretweet_json", action="store_true", help="Generar JSON de corretweets")

    args = parser.parse_args()
    return args.directory, args.fecha_inicial, args.fecha_final, args.hashtags_file, args

def get_tweet_id(tweet):
    return tweet['id_str'] if 'retweeted_status' in tweet else str(tweet['id'])

def validate_tweet_date(tweet, fi, ff):
    if 'created_at' in tweet:
        tweet_date_str = tweet['created_at']
        
        # Convertir la fecha del tweet al formato deseado
        tweet_date = datetime.strptime(tweet_date_str, "%a %b %d %H:%M:%S +0000 %Y").date()
            
        if fi is not None:
            # Convertir la fecha inicial al formato deseado
            fi = datetime.strptime(fi, "%d-%m-%y").date()
            if tweet_date < fi:
                return False

        if ff is not None:
            # Convertir la fecha final al formato deseado
            ff = datetime.strptime(ff, "%d-%m-%y").date()
            if tweet_date > ff:
                return False

        # Si no se generó ninguna excepción, la fecha se validó correctamente
        return True

    return True

def process_original_tweet(tweet, retweets_info, mentions_info, hashtags_set=None, fi=None, ff=None):
    if fi is not None or ff is not None:
        if not validate_tweet_date(tweet, fi, ff):
            # Validation failed, do something or simply return
            return

    if 'user' in tweet:
        tweet_author_username = tweet['user']['screen_name']
        tweet_id = get_tweet_id(tweet)

        if 'entities' in tweet and 'hashtags' in tweet['entities']:
            tweet_hashtags = {tag['text'].lower() for tag in tweet['entities']['hashtags']}
            if hashtags_set and not tweet_hashtags.intersection(hashtags_set):
                return

        retweets_info.setdefault(tweet_author_username, {"tweets": {}})
        retweets_info[tweet_author_username]["tweets"].setdefault(tweet_id, {"retweetedBy": []})

        process_mentions(tweet, mentions_info)

def process_retweet(tweet, retweets_info, mentions_info, hashtags_set=None, fi=None, ff=None):
    if fi is not None or ff is not None:
        if not validate_tweet_date(tweet, fi, ff):
            return

    if 'retweeted_status' in tweet and 'user' in tweet['retweeted_status']:
        retweet_author_username = tweet['retweeted_status']['user']['screen_name']
        retweeted_tweet_id = get_tweet_id(tweet['retweeted_status'])

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


def process_files_in_parallel(file_paths, hashtags_set, fi, ff, rank, size):
    retweets_info = {}
    mentions_info = {}

    if isinstance(file_paths[0], list):
        file_paths = [file_path for sublist in file_paths for file_path in sublist]

    for file_path in file_paths:
        #print(f"Proceso MPI {rank} procesando archivo: {file_path}")
        json_file_path = file_path.with_suffix('.json')

        with bz2.BZ2File(file_path, 'rb') as source, open(json_file_path, 'wb') as target:
            target.write(source.read())

        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            for line in json_file:
                tweet = json.loads(line)
                if 'retweeted_status' in tweet:
                    process_retweet(tweet, retweets_info, mentions_info, hashtags_set, fi, ff)
                else:
                    process_original_tweet(tweet, retweets_info, mentions_info, hashtags_set, fi, ff)

    return retweets_info, mentions_info

def load_hashtags(hashtags_file):
    hashtags_set = set()
    try:
        with open(hashtags_file, 'r') as file:
            hashtags_set = {line.strip().lower() for line in file}
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo de hashtags: {hashtags_file}")
    return hashtags_set

def merge_retweets(retweets_info_list):
    # Inicializar un diccionario predeterminado para evitar comprobaciones adicionales
    merged_results = defaultdict(lambda: {"tweets": defaultdict(lambda: {"retweetedBy": []})})

    # Combinar resultados parciales de diferentes procesos
    for retweets_info_part in retweets_info_list:
        for author_username, author_data in retweets_info_part.items():
            for tweet_id, tweet_data in author_data["tweets"].items():
                # Agregar retweets y retweetedBy sin sobrescribir datos existentes
                merged_results[author_username]["tweets"][tweet_id]["retweetedBy"].extend(tweet_data["retweetedBy"])

    # Convertir el diccionario predeterminado en un diccionario estándar antes de devolverlo
    return dict(merged_results)

def merge_mentions(mentions_info_list):
    merged_results = {}

    for mentions_info_part in mentions_info_list:
        for key, value in mentions_info_part.items():
            merged_results.setdefault(key, {"mentions": []})
            merged_results[key]["mentions"].extend(value["mentions"])

    return merged_results

def generate_retweets_json(retweets_info, arg):
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
    if arg==True:
        with open("rtp.json", "w", encoding="utf-8") as json_file:
            json.dump(retweets_json, json_file, ensure_ascii=False, indent=2)

    return retweets_json

def generate_mentions_json(mentions_info, arg):
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
    if arg==True:
        with open("menciónp.json", "w", encoding="utf-8") as json_file:
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

    nx.write_gexf(G, "rtp.gexf")


def generate_mentions_graph(mentions_json):
    G = nx.Graph()

    for user_data in mentions_json["mentions"]:
        username = user_data["username"]
        
        for mention_data in user_data["mentions"]:
            mention_by = mention_data["mentionBy"]
            G.add_node(username)
            G.add_node(mention_by)
            G.add_edge(username, mention_by)

    nx.write_gexf(G, "menciónp.gexf")


def generate_corrtweets_json(retweets_info, arg):
    corrtweets_dict = defaultdict(set)
    # Recopilar información sobre quién retuiteó a cada autor
    for author, author_info in retweets_info.items():
        for tweet_info in author_info["tweets"].values():
            retweeted_by = tweet_info["retweetedBy"]
        
        # Verificar si tweet_info["retweetedBy"] no está vacío antes de agregar a corrtweets_dict
            if retweeted_by:
                retweeters = set(retweeted_by)
                corrtweets_dict[author].update(retweeters)

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
    if arg==True:
        with open('corrtwp.json', 'w', encoding='utf-8') as json_file:
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

    nx.write_gexf(G, "corrtwp.gexf")

def delete_files(folder_path):
    # Iterate through all the files and subdirectories in the given path
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)

            # Verificar si el archivo tiene la extensión .json
            if file.endswith(".json"):
                try:
                    # Eliminar el archivo
                    os.remove(file_path)
                except Exception as e:
                    print(f"Error deleting {file_path}: {e}")
    
if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    start_time = time.time()
    directory, fecha_inicial, fecha_final, hashtags_file, args = parse_args()

    # Broadcast hashtags_set, fi, ff to all processes
    hashtags_set = None
    if hashtags_file and rank == 0:
        hashtags_set = load_hashtags(hashtags_file)

    # Transmitir hashtags_set, fi, ff a todos los procesos
    hashtags_set = comm.bcast(hashtags_set, root=0)
    fi = comm.bcast(fecha_inicial, root=0)
    ff = comm.bcast(fecha_final, root=0)

    base_path = Path(directory)
    file_paths = list(base_path.rglob('*.json.bz2'))
    local_file_paths = None
    if rank == 0:
        # Divide las rutas de archivos entre los procesos
        chunk_size = len(file_paths) // size
        remainder = len(file_paths) % size
        start_index = 0

        local_file_paths = []
        for i in range(size):
            end_index = start_index + chunk_size + (1 if i < remainder else 0)
            local_file_paths.append(file_paths[start_index:end_index])
            start_index = end_index
    else:
        local_file_paths = None

    # Broadcast de las rutas de archivos a todos los procesos
    local_file_paths = comm.scatter(local_file_paths, root=0)

    # Procesa los archivos asignados a cada proceso
    local_retweets_info, local_mentions_info = process_files_in_parallel(
        local_file_paths, hashtags_set, fi, ff, rank, size
    )

    # Recopila los resultados de todos los procesos
    retweets_info = comm.gather(local_retweets_info, root=0)
    mentions_info = comm.gather(local_mentions_info, root=0)

    # El proceso 0 combina los resultados finales
    if rank == 0:
        merged_results = merge_retweets(retweets_info)
        mentions_results = merge_mentions(mentions_info)

        # Continuar con el resto del código (generación de gráficos, archivos JSON, etc.)
        if args.generate_retweet_graph:
            rt_json = generate_retweets_json(merged_results, args.generate_retweet_json)
            generate_retweets_graph(rt_json)

        if args.generate_mentions_graph:
            mentions_json = generate_mentions_json(mentions_results, args.generate_mentions_json)
            generate_mentions_graph(mentions_json)

        if args.generate_corretweet_graph:
            corrtweets_json = generate_corrtweets_json(merged_results, args.generate_corretweet_json)
            generate_corrtweets_graph(corrtweets_json)

        if args.generate_retweet_json:
            generate_retweets_json(merged_results, args.generate_retweet_json)

        if args.generate_mentions_json:
            generate_mentions_json(mentions_results, args.generate_mentions_json)

        if args.generate_corretweet_json:
            generate_corrtweets_json(merged_results, args.generate_corretweet_json)

        delete_files(directory)
        end_time = time.time()
        total_time = end_time - start_time
        print(f"Proceso completado. Tiempo total de ejecución: {total_time} segundos.")