from queue import Queue
from Graph import *
import sys
from pprint import pprint
import time

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
        self.path_list = []

def main():
    start_time = time.time()
    path = sys.argv[1]
    graph_obj = Graph(path)
    graph = graph_obj.get_graph()

    nodes = {}
    source = 0
    for node in graph:
        nodes[node] = Node(node, source)

    queue = Queue(maxsize = len(graph))
    queue.put(source)

    while not queue.empty():
        parent = queue.get()
        for child in graph[parent]:
            if nodes[child].color == "WHITE":
                nodes[child].color = "GRAY"
                nodes[child].dist = nodes[parent].dist + 1
                nodes[child].path_list.append(parent)
                nodes[child].path_list.extend(nodes[parent].path_list)
                queue.put(child)
        nodes[parent].color = "BLACK"
    
    end_time = time.time()
    for id, node in nodes.items():
        print(f"Node {id}, Color: {node.color}, Dist: {node.dist}, Path List: {node.path_list}")

    print("Total execution time: ", end_time - start_time)
    
if __name__ == "__main__":
    main()