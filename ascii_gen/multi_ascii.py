from datetime import datetime
import os
from os.path import join as pjoin
import math, numbers
import decimal

BUFFER_SIZE = 65536
WATER_LEVEL_FILE = 'water_level.asc'


def get_water_level_grid(lines):
    waterLevels = []
    for line in lines[1:]:
        if line == '\n':
            break
        v = line.split()
        waterLevels.append('%s %s' % (v[0], v[1]))
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
    rows = int(math.ceil((boudary['lat_max'] - boudary['lat_min']) / gap)) + 1
    Grid = [[missingVal for x in range(cols)] for y in range(rows)]

    for level in waterLevels:
        v = level.split()
        i, j = CellMap[int(v[0])]
        water_level = round(decimal.Decimal(v[1]), 2)
        if i >= cols or j >= rows:
            print('i: %d, j: %d, cols: %d, rows: %d' % (i, j, cols, rows))
        if water_level >= min_depth:
            Grid[j][i] = water_level

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


def get_grid_boudary(cadpts_file, gap=250.0):
    """longitude  -> x : larger value
    latitude   -> y : smaller value"""
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


def get_cell_grid(cadpts_file, boudary, gap=250.0):
    CellMap = {}
    cols = int(math.ceil((boudary['long_max'] - boudary['long_min']) / gap)) + 1
    rows = int(math.ceil((boudary['lat_max'] - boudary['lat_min']) / gap)) + 1
    with open(cadpts_file) as f:
        for line in f.readlines():
            v = line.split()
            i = int((float(v[1]) - boudary['long_min']) / gap)
            j = int((float(v[2]) - boudary['lat_min']) / gap)
            if not isinstance(i, numbers.Integral) or not isinstance(j, numbers.Integral):
                print('### WARNING i: %d, j: %d, cols: %d, rows: %d' % (i, j, cols, rows))
            if i >= cols or j >= rows:
                print('### WARNING i: %d, j: %d, cols: %d, rows: %d' % (i, j, cols, rows))
            if i >= 0 or j >= 0:
                CellMap[int(v[0])] = (i, rows - j - 1)
    return CellMap


def generate_ascii_set(ts_start_date, flo2d_output_path, flo2d_model, start_hour=0, end_hour=90, min_depth=0.15):
    print('generate_ascii_set|[ts_start_date, flo2d_output_path, flo2d_model, start_hour, end_hour, min_depth] : ',
          [ts_start_date, flo2d_output_path, flo2d_model, start_hour, end_hour, min_depth])
    buf_size = BUFFER_SIZE
    grid_size = 250
    if flo2d_model == 'flo2d_250':
        grid_size = 250
    elif flo2d_model == 'flo2d_150':
        grid_size = 150
    elif flo2d_model == 'flo2d_30':
        grid_size = 30
    elif flo2d_model == 'flo2d_10':
        grid_size = 10
    timdep_file = os.path.join(flo2d_output_path, 'TIMDEP.OUT')
    cadpts_file = os.path.join(flo2d_output_path, 'CADPTS.DAT')
    ascii_dir = os.path.join(flo2d_output_path, 'multi_ascii')
    if not os.path.exists(ascii_dir):
        os.makedirs(ascii_dir)
    ts_start_date = datetime.strptime(ts_start_date, '%Y-%m-%d')
    with open(timdep_file) as infile:
        waterLevelLines = []
        boundary = get_grid_boudary(cadpts_file, gap=grid_size)
        CellGrid = get_cell_grid(cadpts_file, boundary, gap=grid_size)
        while True:
            lines = infile.readlines(buf_size)
            if not lines:
                break
            for line in lines:
                numbers = line.split('       ')
                if len(numbers) == 1:
                    hour = float(line.strip())
                    if hour >= start_hour:
                        write = True
                    else:
                        write = False
                if write:
                    if len(numbers) == 1:
                        print(line)
                        if len(waterLevelLines) > 0:
                            waterLevels = get_water_level_grid(waterLevelLines)
                            EsriGrid = get_esri_grid(min_depth, waterLevels, boundary, CellGrid, gap=grid_size)
                            fileModelTime = ts_start_date + datetime.timedelta(hours=float(line.strip()))
                            fileModelTime = fileModelTime.strftime('%Y-%m-%d_%H-%M-%S')
                            fileName = WATER_LEVEL_FILE.rsplit('.', 1)
                            fileName = "%s-%s.%s" % (fileName[0], fileModelTime, fileName[1])
                            water_level_file_dir = pjoin(ascii_dir, fileName)
                            file = open(water_level_file_dir, 'w')
                            file.writelines(EsriGrid)
                            file.close()
                            print('Write to :', fileName)
                            waterLevelLines = []
                    else:
                        waterLevelLines.append(line)

