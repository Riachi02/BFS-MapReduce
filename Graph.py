class Graph():
    def __init__(self, file_path):
        self.graph = {}
        with open(file_path, "r") as fp:
            for line in fp.readlines()[2:]:
                pair = line.split()
                pair[0] = int(pair[0])
                pair[1] = int(pair[1])
                if pair[0] in self.graph:
                    self.graph[pair[0]].append(pair[1])
                else:
                    self.graph[pair[0]] = [pair[1]]
                if pair[1] in self.graph:
                    self.graph[pair[1]].append(pair[0])
                else:
                    self.graph[pair[1]] = [pair[0]]

    def get_graph(self):
        return self.graph

