"""Небольшой пример PSIMpy 0.1.3. Для файлов нужен extra streaming."""
from getpass import getpass
from bim_bridge import BimBridge, Element, export_spatial_data


def main():
    bim = BimBridge(token=getpass('Токен BIM Bridge: '))
    model_id = input('GUID модели: ').strip()
    element = Element(input('GUID части: ').strip(), input('GUID элемента: ').strip())
    result = bim.get_spatial_data(model_id, [element], include_descendants=True)
    print('Габариты:', result['Elements'][0]['BoundsMin'], result['Elements'][0]['BoundsMax'])
    # Тот же снимок используется при последующей файловой выгрузке.
    for package in export_spatial_data(bim, model_id, [element], 'outputs/spatial-example',
                                       include_descendants=True, include_meshes=True,
                                       model_version_utc=result['ModelVersionUtc']):
        print('Сохранено:', package['Path'], 'Полнота:', package['Complete'])


if __name__ == '__main__':
    main()
