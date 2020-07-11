import os
import pandas as pd
import math, numbers
import decimal

BUFFER_SIZE = 65536


def get_water_level_grid(lines):
    waterLevels = []
    for line in lines[0:]:
        if line == '\n':
            break
        v = line.split(',')
        index = int(v[0]) + 1
        waterLevels.append('%s %s' % (str(index), v[3]))
    return waterLevels


def get_esri_grid(min_depth, waterLevels, boudary, CellMap, gap=30.0, missingVal=-9999):
    "Esri GRID format : https://en.wikipedia.org/wiki/Esri_grid"
    "ncols         4"
    "nrows         6"
    "xllcorner     0.0"
    "yllcorner     0.0"
    "cellsize      50.0"
    "NODATA_value  -9999"
    "-9999 -9999 5 2"
    "-9999 20 100 36"
    "3 8 35 10"
    "32 42 50 6"
    "88 75 27 9"
    "13 5 1 -9999"

    EsriGrid = []
    cols = int(math.ceil((boudary['long_max'] - boudary['long_min']) / gap)) + 1
    # cols = 492
    rows = int(math.ceil((boudary['lat_max'] - boudary['lat_min']) / gap)) + 1
    # rows = 533
    print('>>>>>  cols: %d, rows: %d' % (cols, rows))
    Grid = [[missingVal for x in range(cols)] for y in range(rows)]
    for level in waterLevels:
        v = level.split()
        i, j = CellMap[int(v[0])]
        water_level = round(decimal.Decimal(v[1]), 2)
        if (i >= cols or j >= rows):
            print('i: %d, j: %d, cols: %d, rows: %d' % (i, j, cols, rows))
        if water_level >= min_depth:
            Grid[j][i] = water_level

    print('ncols:', cols)
    print('nrows:', rows)
    print('xllcorner:', boudary['long_min'] - gap / 2)
    print('yllcorner:', boudary['lat_min'] - gap / 2)
    EsriGrid.append('%s\t%s\n' % ('ncols', cols))
    EsriGrid.append('%s\t%s\n' % ('nrows', rows))
    EsriGrid.append('%s\t%s\n' % ('xllcorner', boudary['long_min'] - gap / 2))
    EsriGrid.append('%s\t%s\n' % ('yllcorner', boudary['lat_min'] - gap / 2))
    EsriGrid.append('%s\t%s\n' % ('cellsize', gap))
    EsriGrid.append('%s\t%s\n' % ('NODATA_value', missingVal))
    for j in range(0, rows):
        arr = []
        for i in range(0, cols):
            arr.append(Grid[j][i])
        EsriGrid.append('%s\n' % (' '.join(str(x) for x in arr)))
    return EsriGrid


def get_grid_boudary(cadpts_file, gap=30.0):
    "longitude  -> x : larger value"
    "latitude   -> y : smaller value"
    long_min = 1000000000.0
    lat_min = 1000000000.0
    long_max = 0.0
    lat_max = 0.0
    with open(cadpts_file) as f:
        lines = f.readlines()
        for line in lines:
            values = line.split()
            long_min = min(long_min, float(values[1]))
            lat_min = min(lat_min, float(values[2]))

            long_max = max(long_max, float(values[1]))
            lat_max = max(lat_max, float(values[2]))
    return {
        'long_min': long_min,
        'lat_min': lat_min,
        'long_max': long_max,
        'lat_max': lat_max
    }


def get_cell_grid(cadpts_file, boudary, gap=30.0):
    CellMap = {}
    cols = int(math.ceil((boudary['long_max'] - boudary['long_min']) / gap)) + 1
    rows = int(math.ceil((boudary['lat_max'] - boudary['lat_min']) / gap)) + 1
    with open(cadpts_file) as f:
        lines = f.readlines()
        for line in lines:
            v = line.split()
            i = int((float(v[1]) - boudary['long_min']) / gap)
            j = int((float(v[2]) - boudary['lat_min']) / gap)
            if not isinstance(i, numbers.Integral) or not isinstance(j, numbers.Integral):
                print('### WARNING i: %d, j: %d, cols: %d, rows: %d' % (i, j, cols, rows))
            if (i >= cols or j >= rows):
                print('### WARNING i: %d, j: %d, cols: %d, rows: %d' % (i, j, cols, rows))
            if i >= 0 or j >= 0:
                CellMap[int(v[0])] = (i, rows - j - 1)
    return CellMap


def create_data_csv(flo2d_output_path, topo_dat_file, maxwselev_file):
    print('create_data_csv|flo2d_output_path:', flo2d_output_path)
    try:
        topo_df = pd.read_csv(topo_dat_file, sep="\s+", names=['x', 'y', 'ground_elv'])

        maxwselev_df = pd.read_csv(maxwselev_file, sep="\s+",
                                   names=['cell_id', 'x', 'y', 'surface_elv']).drop('cell_id', 1)
        maxwselev_df["elevation"] = maxwselev_df["surface_elv"] - topo_df["ground_elv"]

        maxwselev_df.to_csv(os.path.join(flo2d_output_path, 'shape_data.csv'), encoding='utf-8',
                            columns=['x', 'y', 'elevation'], header=False)
        return True
    except Exception as e:
        print("create_data_csv|Exception|e : ", str(e))
        return False


def generate_flood_map(ts_start_date, flo2d_output_path, flo2d_model, start_hour=0, end_hour=90, min_depth=0.15):
    print('generate_flood_map|[ts_start_date, flo2d_output_path, flo2d_model, start_hour, end_hour, min_depth] : ',
          [ts_start_date, flo2d_output_path, flo2d_model, start_hour, end_hour, min_depth])
    grid_size = 250
    buf_size = BUFFER_SIZE
    if flo2d_model == 'flo2d_250':
        grid_size = 250
    elif flo2d_model == 'flo2d_150':
        grid_size = 150
    elif flo2d_model == 'flo2d_30':
        grid_size = 30
    elif flo2d_model == 'flo2d_10':
        grid_size = 10
    topo_dat_file = os.path.join(flo2d_output_path, 'TOPO.DAT')
    maxwselev_file = os.path.join(flo2d_output_path, 'MAXWSELEV.OUT')
    cadpts_file = os.path.join(flo2d_output_path, 'CADPTS.DAT')
    shape_data_csv_file = os.path.join(flo2d_output_path, 'shape_data.csv')
    shape_data_ascii_file = os.path.join(flo2d_output_path, 'max_wl_map.asc')
    if create_data_csv(flo2d_output_path, topo_dat_file, maxwselev_file):
        with open(shape_data_csv_file) as infile:
            waterLevelLines = []
            boundary = get_grid_boudary(cadpts_file, gap=grid_size)
            CellGrid = get_cell_grid(cadpts_file, boundary, gap=grid_size)
            file = open(shape_data_ascii_file, 'w')
            while True:
                lines = infile.readlines(buf_size)
                if not lines:
                    break
                for line in lines:
                    waterLevelLines.append(line)
            waterLevels = get_water_level_grid(waterLevelLines)
            EsriGrid = get_esri_grid(min_depth, waterLevels, boundary, CellGrid, gap=grid_size)
            file.writelines(EsriGrid)
            file.close()
        return True
    else:
        print('generate_flood_map|no data for ascii.')
        return False


if __name__ == '__main__':
    ts_start_date = '2020-05-22'
    flo2d_output_path = '/output/flo2d_output'
    generate_flood_map(ts_start_date, flo2d_output_path, 'flo2d_250')


