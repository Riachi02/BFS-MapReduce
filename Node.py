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

        