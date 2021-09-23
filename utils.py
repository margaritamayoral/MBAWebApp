#This file is for all the functions that the algorithm could need

def hover(hover_color="#ffff99"):
    return dict(selector="tr:hover",
                props=[("background-color", "%s" % hover_color)])

# Defining the hot encoding function t make the data suitable
# for the concerned libraries


def hot_encode(x):
    if(x<= 0):
        return 0
    if(x>= 1):
        return 1

def quote_sql_string(value):
    '''
    If `value` is a string type, escapes single quotes in the string
    and returns the string enclosed in single quotes.
    '''
    if isinstance(value, string_types):
        new_value = str(value)
        new_value = new_value.replace("'", "''")
        return "'{}'".format(new_value)
    return value


def get_sql_from_template(query, bind_params):
    if not bind_params:
        return query
    params = deepcopy(bind_params)
    for key, val in params.items():
        params[key] = quote_sql_string(val)
    return query % params


# Directed graph below is buit for this rule and shown below. It have always incoming and outcoming edges.
## Incoming edge(s) will represent antecedents and the arrow will be next to the node.

# plotting output in a graph  plot

def draw_graph(rules, rules_to_show):
    G1 = nx.DiGraph()
    color_map = []
    N = 50
    colors = np.random.rand(N)
    strs = ['R0', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9', 'R10', 'R11']
    for i in range(rules_to_show):
        G1.add_nodes_from(["R" + str(i)])
        for a in rules.iloc[i - 1]['antecedents']:
            print("a value:", a)
            print("i value:", i)
            print(rules.iloc[i - 1]['antecedents'])
            G1.add_nodes_from([a])
            G1.add_edge(a, "R" + str(i), color=colors[i], weight=2)
            print(G1)
        for c in rules.iloc[i - 1]['consequents']:
            print('c value:', c)
            print(rules.iloc[i - 1]['consequents'])
            G1.add_nodes_from([c])
            G1.add_edge("R" + str(i), c, color=colors[i], weight=2)
    for node in G1:
        found_a_string = False
        for item in strs:
            if node == item:
                found_a_string = True
        if found_a_string:
            color_map.append('yellow')
        else:
            color_map.append('green')

        edges = G1.edges()
        colors = [G1[u][v]['color'] for u, v in edges]
        weights = [G1[u][v]['weight'] for u, v in edges]

        pos = nx.spring_layout(G1, k=16, scale=1)
        nx.draw(G1, node_color=color_map, edge_color=colors, width=weights, font_size=16, with_labels=False)

        for p in pos:  # raise text positions
            pos[p][1] += 0.07
            nx.draw_networkx_labels(G1, pos)
            plt.show()


def create_graph(G,nodes,Sets):
    G.add_edges_from(nodes)

    #value assigned to each world
    custom_labels={}
    custom_node_sizes={}
    node_colours=['y']

    for i in range(0, len(Sets)):
        custom_labels[i+1] = Sets[i]
        custom_node_sizes[i+1] = 5000
        if i < len(Sets):
            node_colours.append('b')
    nx.draw(G,node_list = nodes,node_color=node_colours, node_size=1000, with_labels = True)
    plt.savefig("original_graph.png")
    plt.show()
    G_comp = nx.weakly_connected_component_subgraphs(G)
    i =  1
    for comp in G_comp:
        nx.draw(comp,node_color=node_colours,  node_size=1000, with_labels=True)
        #show with custom labels
        fig_name = "graph" + str(i) + ".png"
        plt.savefig(fig_name)
        plt.show()
        i += 1



