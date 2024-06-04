import ray
import os
import sys
import argparse
from Graph import *
import time
import csv

class Node:

    def __init__(self, id, source):
        self.id = id
        if self.id == source:
            self.color = 'GRAY'
            self.dist = 0
        else:
            self.color = 'WHITE'
            self.dist = float('inf')
        self.neighbours = []
        self.path_list = [source]


@ray.remote
def apply_map(nodes):
    shuffled_nodes = {}
    for node in nodes:
        for id, vertex in map_task(node.id, node):
            if id not in shuffled_nodes:
                shuffled_nodes[id] = {"color": [vertex.color], "dist": [vertex.dist], "neighbours": vertex.neighbours, "path_list": [vertex.path_list]}
            else:
                shuffled_nodes[id]["color"].append(vertex.color)
                shuffled_nodes[id]["dist"].append(vertex.dist)
                for neighbour in vertex.neighbours:
                    if neighbour not in shuffled_nodes[id]["neighbours"]:
                        shuffled_nodes[id]["neighbours"].append(neighbour)
                if vertex.path_list not in shuffled_nodes[id]["path_list"]:
                    shuffled_nodes[id]["path_list"].append(vertex.path_list)
    return shuffled_nodes

def map_task(id, node):
    results = []
    if node.color == "GRAY":
        for neighbour in node.neighbours:
            child = Node(neighbour, 0)
            child.color = "GRAY"
            tmp = []
            for node_in_path in node.path_list:
                tmp.append(node_in_path)
            tmp.append(neighbour)
            child.path_list = tmp
            child.dist = node.dist + 1
            results.append((neighbour, child))
        node.color = "BLACK"
        results.append((id, node))
    else:
        results.append((id, node))
    return results

@ray.remote
def reduce_task(results):
    nodes = []
    for id, values in results.items():
        new_node = Node(id, 0)
        for key, value in values.items():
            if key == 'dist':
                new_node.dist = min(value)
                index = value.index(new_node.dist)
            elif key == 'color':
                new_node.color = set_to_darkest(value)
            elif key == 'path_list':
                new_node.path_list = value[index]
            elif key == 'neighbours':
                new_node.neighbours = value
        nodes.append(new_node)
    return nodes

def set_to_darkest(colors):
    colori_valori = {"WHITE": 0, "GRAY": 1, "BLACK": 2}
    inv_colori = {0: "WHITE", 1: "GRAY", 2: "BLACK"}
    darkest_color = -1
    for color in colors:
        color_value = colori_valori[color]
        if color_value > darkest_color:
            darkest_color = color_value
    return inv_colori[darkest_color]

def partition_graph(graph, num_partitions):
    key_list = list(graph.keys())
    chunk = len(key_list) // num_partitions
    partitions = [
        key_list[i * chunk: (i + 1) * chunk] for i in range(num_partitions)
    ]

    resto = len(key_list) % num_partitions
    if resto != 0: # nel caso in cui la divisione dia resto diverso da 0
        partitions[len(partitions) - 1].extend(key_list[- resto:])
    graph_partitions = []

    for partition in partitions:
        tmp_graph = {}
        for id in partition:
            tmp_graph[id] = graph[id]
        graph_partitions.append(tmp_graph)

    return graph_partitions

def main():
    graph = {}
    nodes, new_nodes = [], []
    parser = argparse.ArgumentParser()
    parser.add_argument("-p","--path")
    parser.add_argument("-w","--workers")
    args = parser.parse_args()
    path, num_partitions = args.path, int(args.workers)

    start_time = time.time()
    graph_obj = Graph(path)
    graph = graph_obj.get_graph()
    graph_partitions = partition_graph(graph, num_partitions)
    print(graph)
    for graph in graph_partitions:
        nodes = []
        for key, value in graph.items():
            node = Node(key, 0)
            node.neighbours = value
            nodes.append(node)
        new_nodes.append(nodes)

    ray.init()
    blacks = 0
    while True:
        blacks = 0
        map_results, futures = [], []
        shuffled_nodes = {}

        # MAP
        futures = [apply_map.remote(nodes_list) for nodes_list in new_nodes]

        results = ray.get(futures)
        
        # SHUFFLE
        for result_dict in results:
            for id, vertex in result_dict.items():
                if id not in shuffled_nodes:
                    shuffled_nodes[id] = {"color": vertex["color"], "dist": vertex["dist"], "neighbours": vertex["neighbours"], "path_list": vertex["path_list"]}
                else:
                    shuffled_nodes[id]["color"].extend(vertex["color"])
                    shuffled_nodes[id]["dist"].extend(vertex["dist"])
                    for neighbour in vertex["neighbours"]:
                        if neighbour not in shuffled_nodes[id]["neighbours"]:
                            shuffled_nodes[id]["neighbours"].append(neighbour)
                    if vertex["path_list"] not in shuffled_nodes[id]["path_list"]:
                        shuffled_nodes[id]["path_list"].extend(vertex["path_list"])
        
        map_results = partition_graph(shuffled_nodes, num_partitions)

        # REDUCE
        outputs = [reduce_task.remote(map_results[i]) for i in range(num_partitions)]

        nodes_res = ray.get(outputs)
        new_nodes, all_nodes = [], []
        for nodes_list in nodes_res:
            new_nodes.append(nodes_list)
            all_nodes.extend(nodes_list)

        for node in all_nodes:
            #print(node.id, node.color, node.dist, node.path_list)
            if node.color == 'BLACK':
                blacks += 1

        print("Num blacks: ", blacks)
        if all(node.color == 'BLACK' for node in all_nodes): # exit condition
            break

    end_time = time.time()
    for node_list in nodes_res:
        for node in node_list:
            print(node.id, node.color, node.dist, node.path_list)

    # Definire i nomi dei campi
    fieldnames = ['Dataset', 'n_Workers', 'Tempo']
    file_path = 'results.csv'
    file_exist = os.path.isfile(file_path)
    with open(file_path, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        # Scrivere l'intestazione solo se il file non esiste
        if not file_exist:
            writer.writeheader()
        
        # Scrivere i valori
        writer.writerow({'Dataset': path, 'n_Workers': num_partitions, 'Tempo': end_time - start_time})
    
    print("Total execution time: ", end_time - start_time)

if __name__ == "__main__":
    main()
