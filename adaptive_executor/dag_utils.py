import sqlite3, logging, os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#O(n*m), onde n é o número de nós e m o número de arestas
def load_graph(run_id, df):
    dag = nx.DiGraph()
    df_run = df[df["run_id"] == run_id]
    df_run = df_run.sort_values(by=['task_id'], ascending=[True])
    if df_run.empty:
        return None
    else:
        #print(df_run)
        tasks = df_run[["task_id", "task_func_name", "runtime_seconds", "task_depends"]]
        for i, r in tasks.iterrows():
            task_id = r["task_id"]
            task_func_name = r["task_func_name"]
            runtime = r["runtime_seconds"]
            depends_on = (r["task_depends"]).split(',')
            depends_on = list(filter((lambda x : len(x)>0), depends_on))
            dag.add_node(task_id, task_func_name=task_func_name, runtime=runtime)
            #print(depends_on)
            for d in depends_on:
                dag.add_edge(int(d), task_id)
        #draw_dag(dag)
        return dag
    

def load_most_similar_dag(old_dag, df, task_id, task_func_name):
    if old_dag is not None and len(old_dag.nodes) > task_id and old_dag.nodes[task_id]["task_func_name"] == task_func_name:
        logger.info("Old DAG seems to be the most similar to the current task")
        return old_dag

    if df is None:
        logger.warning("No DAG dataframe (df) provided.")
        return old_dag

    filtered_df = df[(df["task_id"] == task_id) & (df["task_func_name"] == task_func_name)]
    if filtered_df.empty:
        # logger.info("There is no DAG with the current task")
        return old_dag
    else:
        filtered_df = filtered_df.drop_duplicates(subset=['task_id'])  # select only one of each task_id
        # logger.info("Loading a new DAG")
        r = filtered_df.iloc[0]
        return load_graph(r["run_id"], df)

        

def load_df_from_db(db_path = None, run_dir = "./runinfo"):
    df = None
    if db_path == None:
        db_path = os.path.abspath(run_dir) 
    monitoring_db_file = os.path.join(db_path, "monitoring.db")
    if os.path.exists(monitoring_db_file):
        logger.debug("Monitoring.db found!")
        try:
            with sqlite3.connect(monitoring_db_file) as connection:
                    select_query = f"SELECT * FROM task"
                    df = pd.read_sql_query(select_query, connection)
                    df = df[df['task_time_returned'].notnull() & df['task_time_invoked'].notnull()] #select only the items with valid timestamps
                    df['task_time_returned'] = pd.to_datetime(df['task_time_returned'], errors='coerce')
                    df['task_time_invoked'] = pd.to_datetime(df['task_time_invoked'], errors='coerce')
                    df = df[df['task_time_returned'].notna() & df['task_time_invoked'].notna()] #drop NaT items
                    df.loc[:, 'runtime'] = df['task_time_returned'] - df['task_time_invoked']
                    df.loc[:, 'runtime_seconds'] = df['runtime'].dt.total_seconds()
                    return df
        except:
            return None
        


# hierarchical layout drawing
#Source: https://stackoverflow.com/questions/29586520/can-one-get-hierarchical-graphs-from-networkx-with-python-3
def hierarchy_pos(G, root, levels=None, width=1., height=1.):
    '''If there is a cycle that is reachable from root, then this will see infinite recursion.
       G: the graph
       root: the root node
       levels: a dictionary
               key: level number (starting from 0)
               value: number of nodes in this level
       width: horizontal space allocated for drawing
       height: vertical space allocated for drawing'''
    TOTAL = "total"
    CURRENT = "current"
    def make_levels(levels, node=root, currentLevel=0, parent=None):
        """Compute the number of nodes for each level
        """
        if not currentLevel in levels:
            levels[currentLevel] = {TOTAL : 0, CURRENT : 0}
        levels[currentLevel][TOTAL] += 1
        neighbors = G.neighbors(node)
        for neighbor in neighbors:
            if not neighbor == parent:
                levels =  make_levels(levels, neighbor, currentLevel + 1, node)
        return levels

    def make_pos(pos, node=root, currentLevel=0, parent=None, vert_loc=0):
        dx = 1/levels[currentLevel][TOTAL]
        left = dx/2
        pos[node] = ((left + dx*levels[currentLevel][CURRENT])*width, vert_loc)
        levels[currentLevel][CURRENT] += 1
        neighbors = G.neighbors(node)
        for neighbor in neighbors:
            if not neighbor == parent:
                pos = make_pos(pos, neighbor, currentLevel + 1, node, vert_loc-vert_gap)
        return pos
    if levels is None:
        levels = make_levels({})
    else:
        levels = {l:{TOTAL: levels[l], CURRENT:0} for l in levels}
    vert_gap = height / (max([l for l in levels])+1)
    return make_pos({})

def draw_dag(dag, dir):
    pos = hierarchy_pos(dag, root = 0)
    fig, ax = plt.subplots()
    nx.draw_networkx(dag, pos=pos, ax=ax)
    ax.set_title("DAG layout in topological order")
    fig.tight_layout()
    fig.savefig(os.path.join(dir, "dag.png"), dpi=300)
