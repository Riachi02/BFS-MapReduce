import ray
import os
import sys
import pprint
#from Node import *
from Graph import *

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
    path, num_partitions = sys.argv[1], int(sys.argv[2])

    graph_obj = Graph(path)
    graph = graph_obj.get_graph()
    graph_partitions = partition_graph(graph, num_partitions)
   
    for graph in graph_partitions:
        nodes = []
        for key, value in graph.items():
            node = Node(key, 0)
            node.neighbours = value
            nodes.append(node)
        new_nodes.append(nodes)

    while True:
        map_results, futures = [], []
        shuffled_nodes = {}

        # MAP

        futures = [map_task.remote(node.id, node) for nodes_list in new_nodes for node in nodes_list]

        '''for nodes_list in new_nodes:
            for node in nodes_list:
                futures.append(map_task.remote(node.id, node))'''

        results = ray.get(futures)
        
        # SHUFFLE
        for result in results:
            for id, vertex in result:
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
        
        map_results = partition_graph(shuffled_nodes, num_partitions)

        # REDUCE
        '''outputs = []
        for i in range(num_partitions):
            outputs.append(reduce_task.remote(map_results[i]))'''
        
        outputs = [reduce_task.remote(map_results[i]) for i in range(num_partitions)]

        nodes_res = ray.get(outputs)
        new_nodes, all_nodes = [], []
        for nodes_list in nodes_res:
            new_nodes.append(nodes_list)
            all_nodes.extend(nodes_list)

        if all(node.color == 'BLACK' for node in all_nodes): # exit condition
            break

    for node_list in nodes_res:
        for node in node_list:
            print(node.id, node.color, node.dist, node.path_list)

if __name__ == "__main__":
    main()
