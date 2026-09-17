"""Полная последовательная выгрузка свойств выбранных частей в JSONL.

Импорт в Colab:
    from bim_bridge import ...
Функцию export можно скопировать в ячейку или импортировать из этого файла.
При импорте никакие запросы не выполняются.
"""
from bim_bridge import Checkpoints, iter_search_elements, iter_properties, write_jsonl


def export(client, model_id, part_ids, *, output="outputs/properties.jsonl",
           checkpoint_dir="checkpoints/export-001", batch_size=500):
    checkpoint = Checkpoints(client, checkpoint_dir)
    ids = iter_search_elements(client, model_id, part_ids, checkpoints=checkpoint)
    batches = iter_properties(client, model_id, ids, batch_size=batch_size,
                              checkpoints=checkpoint)
    return write_jsonl((row for batch in batches for row in batch["Elements"]), output)


if __name__ == "__main__":
    import argparse
    from getpass import getpass
    from bim_bridge import BimBridge
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("model_id", help="GUID консолидированной модели")
    parser.add_argument("part_ids", nargs="+", help="GUID частей этой модели")
    parser.add_argument("--url", default="https://360pilot.ru/bim-bridge")
    parser.add_argument("--output", default="outputs/properties.jsonl")
    parser.add_argument("--checkpoints", default="checkpoints/export-001")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    client = BimBridge(args.url, token=getpass("Токен BIM Bridge: "))
    print(export(client, args.model_id, args.part_ids, output=args.output,
                 checkpoint_dir=args.checkpoints, batch_size=args.batch_size))
