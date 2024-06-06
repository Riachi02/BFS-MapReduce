import ray
import os
import argparse
from Graph import *
import time
import csv

class Node:

    def __init__(self, id, source):
        self.id = id
        if self.id == source: 
            self.color = 'GRAY' # Il nodo sorgente è il primo che viene esplorato
            self.dist = 0
        else:
            self.color = 'WHITE' 
            self.dist = float('inf')
        self.neighbours = []
        self.path_list = [source] # Tutti i nodi hanno il nodo sorgente all'interno della lista del percorso dal nodo sorgente specificato


@ray.remote
def apply_map(nodes):
    shuffled_nodes = {} # Struttura dati utilizzata per mantenere i nodi ordinati in base al loro identificativo
    for node in nodes: # Iterazione sulla lista degli oggetti di tipo Node
        for id, vertex in map_task(node.id, node): # Shuffling all'interno di ogni worker 
            if id not in shuffled_nodes: # Ad ogni identificativo (chiave) viene associato un dizionario (valore) che contiene le caratteristiche del nodo
                shuffled_nodes[id] = {"color": [vertex.color], "dist": [vertex.dist], "neighbours": vertex.neighbours, "path_list": [vertex.path_list]}
            else:
                shuffled_nodes[id]["color"].append(vertex.color) # Se il nodo è già presente nel dizionario, vengono inserite le caratteristiche di questo emesse dalla fase di mapping 
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
        for neighbour in node.neighbours: # Iterazione sui vicini del nodo, i quali non sono ancora stati creati come oggetti di tipo Node
            child = Node(neighbour, 0) # Creazione del nodo
            child.color = "GRAY" # Aggiornamento dello stato del nodo, in modo tale che venga esplorato alla successiva iterazione
            tmp = []
            for node_in_path in node.path_list: # Aggiornamento del percorso dal nodo sorgente, aggiungendo se stesso al percorso del nodo genitore
                tmp.append(node_in_path)
            tmp.append(neighbour)
            child.path_list = tmp
            child.dist = node.dist + 1 # Incremento della distanza dal nodo sorgente sulla base della distanza mantenuta dal nodo genitore
            results.append((neighbour, child)) # Emissione della tupla caratterizzata dall'identificativo del nodo e l'oggetto nodo 
        node.color = "BLACK" # Aggiornamento dello stato del nodo ad 'esplorato'
        results.append((id, node)) 
    else:
        results.append((id, node))
    return results

@ray.remote
def reduce_task(results):
    nodes = []
    for id, values in results.items(): 
        new_node = Node(id, 0)
        for key, value in values.items(): # Iterazione sulle caratteristiche del nodo
            if key == 'dist':
                new_node.dist = min(value) # La distanza dal nodo sorgente viene impostata al minimo dall'insieme dei valori ottenuti
                index = value.index(new_node.dist) # Ne viene individuato l'indice
            elif key == 'color':
                new_node.color = set_to_darkest(value) # Il colore viene aggiornato considerando quello più scuro dall'insieme ottenuto
            elif key == 'path_list':
                new_node.path_list = value[index] # Il percorso dal nodo sorgente viene impostato sulla base dell'indice della distanza minima trovata
            elif key == 'neighbours':
                new_node.neighbours = value
        nodes.append(new_node)
    return nodes

def set_to_darkest(colors):
    colori_valori = {"WHITE": 0, "GRAY": 1, "BLACK": 2} 
    inv_colori = {0: "WHITE", 1: "GRAY", 2: "BLACK"}
    darkest_color = -1 # Variabile di appoggio
    for color in colors: # Iterazione sulla lista dei colori associati al nodo e ottenuti dalla fase di mapping
        color_value = colori_valori[color]
        if color_value > darkest_color: 
            darkest_color = color_value 
    return inv_colori[darkest_color] # Viene restituito il colore più scuro 

def partition_graph(graph, num_partitions):
    key_list = list(graph.keys())
    chunk = len(key_list) // num_partitions # Calcolo della dimensione della singola partizione
    partitions = [
        key_list[i * chunk: (i + 1) * chunk] for i in range(num_partitions)
    ] # Creazione delle partizioni sulla base della dimensione di ciascuno

    resto = len(key_list) % num_partitions
    if resto != 0: # Nel caso in cui la divisione dia resto diverso da 0, i nodi rimanenti vengono aggiunti all'ultima partizione
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
    graph_obj = Graph(path) # Creazione dell'oggetto grafo
    graph = graph_obj.get_graph() 
    graph_partitions = partition_graph(graph, num_partitions) # Suddivisione del grafo

    for graph in graph_partitions:
        nodes = []
        for key, value in graph.items(): # Creazione degli oggetti nodo all'interno di tutti i grafi
            node = Node(key, 0)
            node.neighbours = value # Al singolo nodo viene associata la sua lista di vicini
            nodes.append(node)
        new_nodes.append(nodes)

    ray.init()
    while True:
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
        for nodes_list in nodes_res: # Aggiornamento della lista che mantiene l'insieme dei nodi ad ogni iterazione
            new_nodes.append(nodes_list)
            all_nodes.extend(nodes_list)

        if all(node.color == 'BLACK' for node in all_nodes): # Condizione di uscita 
            break

    end_time = time.time()
    
    # Gestione del file che memorizza i tempi registrati
    fieldnames = ['Dataset', 'n_Workers', 'Tempo'] # Intestazioni dei campi
    file_path = 'results.csv'
    file_exist = os.path.isfile(file_path)
    with open(file_path, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        if not file_exist: # Le intestazioni vengono scritte solo se il file non esiste
            writer.writeheader()
        
        writer.writerow({'Dataset': path, 'n_Workers': num_partitions, 'Tempo': end_time - start_time})
    
    print("Total execution time: ", end_time - start_time)

if __name__ == "__main__":
    main()
