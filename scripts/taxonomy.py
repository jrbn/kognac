from graphviz import Digraph
from bisect import bisect_right


class Taxonomy:
    edges = {}
    nodes = {}
    ranges = []

    # Private methods
    def _cleanup(input):
        if input.startswith('<'):
            input = input[1:-1]
            if input.startswith('http://www.w3.org/2002/07/owl#'):
                input = 'owl_' + input[input.find('#')+1:]
            if input.startswith('http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#'):
                input = 'lubm_' + input[input.find('#')+1:]
            if input.startswith('http://www.w3.org/2000/01/rdf-schema#'):
                input = 'rdfs_' + input[input.find('#')+1:]
            if input.startswith('_:'):
                input = 'blanknode_' + input[2:]
        return input

    # Public methods
    def __init__(self, inputfile):
        rang = False
        subc = False
        # Load the file
        for line in open(inputfile):
            if "#Ranges#" in line:
                subc = False
                rang = True
            elif "#Taxonomy#" in line:
                subc = True
                rang = False
            elif rang:
                line = line[:-1]
                tok = line.split('\t')
                self.nodes[tok[0]] = tok[1]
                if len(tok) > 2:
                    self.ranges.append((tok[1], tok[2], tok[3]))
            elif subc:
                line = line[:-1]
                tok = line.split('\t')
                source = tok[0]
                dest = tok[1]
                source = self._cleanup(source)
                dest = self._cleanup(dest)
                if source not in self.edges:
                    self.edges[source] = []
                    self.edges[source].append(dest)
        # Sort the ranges
        sorted(self.ranges, key=lambda x: (x == 0, x))

    def getClass(self, idInstance):
        pos = bisect_right(self.ranges, idInstance, 0, None)
        return self.ranges[pos][0]

    def visualize(self):
        # Create the graph
        dot = Digraph(comment='Taxonomy')
        for k, v in self.nodes.items():
            node = self._cleanup(k)
            dot.node(node, label=node + '(' + str(v) + ')')
            for k, v in self.edges.items():
                for dest in v:
                    dot.edge(k, dest)
                    dot.render(view=True)
