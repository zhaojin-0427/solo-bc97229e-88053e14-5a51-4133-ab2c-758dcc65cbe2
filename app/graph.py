"""有向图工具：环检测（迭代 DFS，避免深图递归栈溢出）。

边以 (parent, child) 文本对表示，方向 parent -> child，语义为
“parent 参与产生 child”。
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Iterator

# 邻接表：节点 -> 直接后继（衍生自该节点的样本号）
Graph = dict[str, set[str]]


def build_graph(edges: Iterable[tuple[str, str]]) -> Graph:
    graph: Graph = defaultdict(set)
    for parent, child in edges:
        graph[parent].add(child)
        graph.setdefault(child, set())
    return graph


def merge_edges(
    base: Graph, new_edges: Iterable[tuple[str, str]]
) -> Graph:
    """把一批候选边合并进基础图，返回新图（不修改入参）。"""
    merged: Graph = defaultdict(set)
    for node, succs in base.items():
        merged[node] = set(succs)
    for parent, child in new_edges:
        merged.setdefault(parent, set()).add(child)
        merged.setdefault(child, set())
    return merged


def find_cycle(graph: Graph) -> list[str] | None:
    """返回一条有向环，节点按环顺序排列（首尾相同），无环返回 None。

    自环返回形如 ["X", "X"]；普通环如 ["A", "B", "C", "A"]。
    使用 WHITE/GRAY/BLACK 三色迭代 DFS，显式维护栈帧，
    避免对深度上万的谱系图触发 Python 递归上限。
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = dict.fromkeys(graph, WHITE)

    for start in graph:
        if color[start] != WHITE:
            continue
        # 栈帧为 (节点, 该节点尚未访问的后继迭代器)
        stack: list[tuple[str, Iterator]] = [(start, iter(sorted(graph[start])))]
        color[start] = GRAY
        path = [start]

        while stack:
            node, it = stack[-1]
            advanced = False
            for nxt in it:
                if color.get(nxt, WHITE) == GRAY:
                    # 回到当前 DFS 路径上的节点 => 找到环
                    idx = path.index(nxt)
                    return [*path[idx:], nxt]
                if color.get(nxt, WHITE) == WHITE:
                    color[nxt] = GRAY
                    path.append(nxt)
                    stack.append((nxt, iter(sorted(graph.get(nxt, set())))))
                    advanced = True
                    break
                # BLACK：后继已完整展开，跳过
            if not advanced:
                color[node] = BLACK
                stack.pop()
                path.pop()
    return None
