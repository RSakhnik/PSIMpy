"""Worker-side bulk properties export; importing this module makes no requests."""


def export(client, model_id, part_ids, *, output="outputs/properties.ndjson.gz",
           property_names=None):
    return client.export_elements(model_id, output, part_ids=part_ids,
                                  property_names=property_names).path


if __name__ == "__main__":
    import argparse
    from getpass import getpass
    from bim_bridge import BimBridge
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("server_database")
    parser.add_argument("login")
    parser.add_argument("model_id")
    parser.add_argument("part_ids", nargs="+")
    parser.add_argument("--output", default="outputs/properties.ndjson.gz")
    args = parser.parse_args()
    client = BimBridge.from_pilot(args.server_database,args.login,getpass("Pilot password: "))
    try:
        print(export(client,args.model_id,args.part_ids,output=args.output))
    finally:
        client.close()
