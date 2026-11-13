"""图算法单元测试：环检测（含自环）、合并。"""

from itertools import pairwise

from app.graph import build_graph, find_cycle, merge_edges


def test_acyclic_graph_returns_none():
    g = build_graph([("A", "B"), ("A", "C"), ("B", "D"), ("C", "D")])
    assert find_cycle(g) is None


def test_direct_cycle_found_and_is_real_path():
    g = build_graph([("A", "B"), ("B", "C"), ("C", "A")])
    cycle = find_cycle(g)
    assert cycle is not None
    assert cycle[0] == cycle[-1]
    # 环上每一步都必须是真实存在的边
    for u, v in pairwise(cycle):
        assert v in g[u]


def test_self_loop_detected():
    g = build_graph([("X", "X"), ("X", "Y")])
    assert find_cycle(g) == ["X", "X"]


def test_merge_does_not_mutate_base():
    base = build_graph([("A", "B")])
    merged = merge_edges(base, [("B", "C")])
    assert "C" not in base.get("B", set())
    assert "C" in merged["B"]


def test_long_chain_no_recursion_problem():
    edges = [(f"N{i}", f"N{i+1}") for i in range(5000)]
    g = build_graph(edges)
    assert find_cycle(g) is None
    g["N4999"].add("N0")  # 制造一个 5001 节点的大环
    cycle = find_cycle(g)
    assert cycle is not None and cycle[0] == cycle[-1]


def test_diamond_dedup_view():
    # 菱形：A -> B,D; B -> C; D -> C，无环
    g = build_graph([("A", "B"), ("A", "D"), ("B", "C"), ("D", "C")])
    assert find_cycle(g) is None
