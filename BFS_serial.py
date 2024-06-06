from queue import Queue
from Graph import *
from Node import *
import sys
import time

start_time = time.time()
path = sys.argv[1]
graph_obj = Graph(path) # Creazione dell'oggetto grafo
graph = graph_obj.get_graph() # Restituisce il grafo come dizionario

nodes = {}
source = 0 # Nodo sorgente
for node in graph:
    nodes[node] = Node(node, source) # Creazione della lista di oggetti nodi

queue = Queue(maxsize = len(graph))
queue.put(source) 

while not queue.empty(): # Il ciclo si interrompe quando la coda risulta essere vuota
    parent = queue.get()
    for child in graph[parent]: # Itera la lista di adiacenza del nodo
        if nodes[child].color == "WHITE": 
            nodes[child].color = "GRAY" # Aggiornamento delle caratteristiche dei vicini del nodo
            nodes[child].dist = nodes[parent].dist + 1 
            nodes[child].path_list.append(parent)
            nodes[child].path_list.extend(nodes[parent].path_list)
            queue.put(child)
    nodes[parent].color = "BLACK" # Aggiornamento dello stato del nodo ad "esplorato"

end_time = time.time()

print("Total execution time: ", end_time - start_time)
    