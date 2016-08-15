import sys
import sets
from graphviz import Digraph

taxonomy = sys.argv[1]

rang = False
subc = False

edges = {}
nodes = {}

def cleanup(input):
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

for line in open(taxonomy):
    if "#Ranges#" in line:
        subc = False
        rang = True
    elif "#Taxonomy#" in line:
        subc = True
        rang = False
    elif rang:
        line = line[:-1]
        tok = line.split('\t')
        nodes[tok[0]] = tok[1]
    elif subc:
        line = line[:-1]
        tok = line.split('\t')
        source = tok[0]
        dest = tok[1]
        source = cleanup(source)
        dest = cleanup(dest)
        if source not in edges:
            edges[source] = []
        edges[source].append(dest)

#Create the graph
dot = Digraph(comment='Taxonomy')
for k,v in nodes.items():
    node = cleanup(k)
    dot.node(node, label=node + '(' + str(v) + ')')
for k,v in edges.items():
    for dest in v:
        dot.edge(k,dest)
dot.render(view=True)
