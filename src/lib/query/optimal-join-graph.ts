import graphlib from "@dagrejs/graphlib";
import invariant from "tiny-invariant";

function makeSingleTableGraph(tableName: string) {
  const graph = new graphlib.Graph();
  graph.setNode(tableName);
  return graph;
}

function buildCompleteGraph(
  paths: Record<string, Record<string, graphlib.Path>>,
  requestedTables: string[],
) {
  const completeGraph = new graphlib.Graph();

  // Initialize the complete graph with nodes
  for (const table of requestedTables) {
    completeGraph.setNode(table);
  }

  // Set edges based on shortest paths between all pairs of requested tables
  for (let i = 0; i < requestedTables.length; i++) {
    for (let j = i + 1; j < requestedTables.length; j++) {
      const start = requestedTables[i];
      const end = requestedTables[j];
      invariant(start, `Start table: ${start} not found`);
      invariant(end, `End table: ${end} not found`);

      // Ensure there is a path between start and end
      if (paths[start]?.[end] && paths[start]?.[end]?.distance !== Infinity) {
        const weight = paths[start]?.[end]?.distance;
        completeGraph.setEdge(start, end, weight);
      }
    }
  }

  return completeGraph;
}

export function findOptimalJoinGraph(
  originalGraph: graphlib.Graph,
  requestedTables: string[],
) {
  if (requestedTables.length === 1) {
    return makeSingleTableGraph(requestedTables[0]!);
  }

  const paths = graphlib.alg.dijkstraAll(originalGraph);
  const completeGraph = buildCompleteGraph(paths, requestedTables);
  const mst = graphlib.alg.prim(completeGraph, (e) => {
    return completeGraph.edge(e);
  });

  const joinGraph = new graphlib.Graph();
  const edges = mst.edges();

  for (const { v, w } of edges) {
    if (!paths[v]?.[w] || paths[v]?.[w]?.distance === Infinity) {
      throw new Error(
        `No path exists between ${v} and ${w} in the original graph.`,
      );
    }

    const path = [w];
    let currentNode: string | undefined = w;

    // Safely reconstruct the shortest path from w to v
    while (currentNode !== v) {
      currentNode = paths[v]?.[currentNode]?.predecessor;
      if (currentNode === undefined) {
        throw new Error(`Path reconstruction failed from ${v} to ${w}.`);
      }
      path.unshift(currentNode);
    }

    // Add the nodes and edges of this path to the join graph
    for (let k = 1; k < path.length; k++) {
      const u = path[k - 1];
      const v = path[k];
      invariant(u, `Node ${u} not found`);
      invariant(v, `Node ${v} not found`);
      joinGraph.setEdge(u, v, originalGraph.edge(u, v));
    }
  }

  return joinGraph;
}
