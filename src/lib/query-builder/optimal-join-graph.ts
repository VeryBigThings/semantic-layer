import graphlib from "@dagrejs/graphlib";
import invariant from "tiny-invariant";

function makeSingleModelGraph(modelName: string) {
  const graph = new graphlib.Graph();
  graph.setNode(modelName);
  return graph;
}

function buildCompleteGraph(
  paths: Record<string, Record<string, graphlib.Path>>,
  requestedModels: string[],
) {
  const completeGraph = new graphlib.Graph();

  // Initialize the complete graph with nodes
  for (const model of requestedModels) {
    completeGraph.setNode(model);
  }

  // Set edges based on shortest paths between all pairs of requested models
  for (let i = 0; i < requestedModels.length; i++) {
    for (let j = i + 1; j < requestedModels.length; j++) {
      const start = requestedModels[i];
      const end = requestedModels[j];
      invariant(start, `Start model: ${start} not found`);
      invariant(end, `End model: ${end} not found`);

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
  requestedModels: string[],
) {
  if (requestedModels.length === 1) {
    return makeSingleModelGraph(requestedModels[0]!);
  }

  const paths = graphlib.alg.dijkstraAll(originalGraph);
  const completeGraph = buildCompleteGraph(paths, requestedModels);
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
