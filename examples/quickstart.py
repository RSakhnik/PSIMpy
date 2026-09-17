"""Небольшая проверка подключения. При импорте запросы не отправляются."""
from bim_bridge import BimBridge, Condition


def inspect_part(client, model_id, part_id):
    result = client.search_elements(model_id, [Condition("Name", "Defined")],
                                    part_ids=[part_id], max_results=3)
    elements = result["Elements"]
    return {
        "search": result,
        "properties": client.load_properties(model_id, elements) if elements else None,
        "relations": client.get_relations(model_id, elements) if elements else None,
    }


if __name__ == "__main__":
    from getpass import getpass
    client = BimBridge(token=getpass("Токен BIM Bridge: "))
    print(client.health())
    print(client.workers())
