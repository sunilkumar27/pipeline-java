from fastapi import FastAPI, Form
from pydantic import BaseModel
from typing import List, Dict, Tuple

app = FastAPI()

class Node(BaseModel):
    id: str

class Edge(BaseModel):
    source: str
    target: str

class Pipeline(BaseModel):
    nodes: List[Node]
    edges: List[Edge]

@app.get("/")
def read_root():
    return {"Ping": "Pong"}

def is_dag(nodes: List[str], edges: List[Tuple[str, str]]) -> bool:
    # Check if the graph is a DAG using topological sort
    from collections import defaultdict, deque

    in_degree = defaultdict(int)
    adjacency_list = defaultdict(list)

    # Initialize graph
    for edge in edges:
        adjacency_list[edge[0]].append(edge[1])
        in_degree[edge[1]] += 1

    # Nodes with no incoming edges
    zero_in_degree = deque([node for node in nodes if in_degree[node] == 0])
    visited_count = 0

    # Perform topological sort
    while zero_in_degree:
        current = zero_in_degree.popleft()
        visited_count += 1

        for neighbor in adjacency_list[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                zero_in_degree.append(neighbor)

    return visited_count == len(nodes)

@app.post("/pipelines/parse")
def parse_pipeline(pipeline: Pipeline):
    nodes = [node.id for node in pipeline.nodes]
    edges = [(edge.source, edge.target) for edge in pipeline.edges]

    num_nodes = len(nodes)
    num_edges = len(edges)
    dag_status = is_dag(nodes, edges)

    return {"num_nodes": num_nodes, "num_edges": num_edges, "is_dag": dag_status}
