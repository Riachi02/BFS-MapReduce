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