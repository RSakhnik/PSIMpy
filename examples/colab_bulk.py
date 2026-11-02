"""Colab cells: install SDK in /content, then connect through Pilot Custom API.

The setup function is explicit: importing this module does not download anything,
start a client, authenticate, or submit a task.
"""
from pathlib import Path
import os
import subprocess
import urllib.request


def install_colab_dotnet(directory='/content/dotnet'):
    directory=Path(directory)
    if not (directory/'dotnet').exists():
        script=Path('/content/dotnet-install.sh')
        urllib.request.urlretrieve('https://dot.net/v1/dotnet-install.sh',script)
        subprocess.run(['bash',str(script),'--channel','9.0','--install-dir',str(directory),'--no-path'],check=True)
    os.environ['DOTNET_ROOT']=str(directory)
    os.environ['PATH']=str(directory)+os.pathsep+os.environ.get('PATH','')


def export_selected_properties(client,model_id,part_ids,output):
    artifact=client.export_elements(model_id,output,part_ids=part_ids,property_names=['Name','GlobalId','Mass'])
    for frame in artifact.iter_dataframes(chunk_size=10000):
        yield frame


def export_meshes(client,model_id,part_ids,output):
    artifact=client.export_spatial(model_id,output,part_ids=part_ids,include_meshes=True,
                                   include_normals=False,include_indices=True,include_edge_indices=False)
    reader=artifact.spatial()
    reader.validate()
    return reader
