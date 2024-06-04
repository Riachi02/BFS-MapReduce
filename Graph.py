class Graph():
    def __init__(self, file_path): # path del file contenente il dataset
        self.graph = {}
        with open(file_path, "r") as fp:
            for line in fp.readlines(): # per ogni riga del file
                pair = line.split() # suddivisione della riga in un array di due elementi
                pair[0] = int(pair[0]) # casting del char a intero
                pair[1] = int(pair[1])
                if pair[0] in self.graph: # inserimento del secondo nodo alla lista di adiacenza del primo
                    self.graph[pair[0]].append(pair[1])
                else:
                    self.graph[pair[0]] = [pair[1]]
                if pair[1] in self.graph: # inserimento del primo nodo alla lista di adiacenza del secondo
                    self.graph[pair[1]].append(pair[0])
                else:
                    self.graph[pair[1]] = [pair[0]]

    def get_graph(self): # metodo per ottenere il grafo generato
        return self.graph

