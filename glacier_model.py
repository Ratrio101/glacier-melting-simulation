#!/usr/bin/env python3

# Модули и библиотеки, необходимые для работы программы
import os
import sys
import math
import tempfile
import datetime as dt
from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio

# ===== НАСТРОЙКА GRASS GIS =====
grass_base = r"C:\GRASS" # база GRASS GIS

# Проверка на наличие базы GRASS GIS
if not os.path.exists(grass_base):
    print(f"✗ GRASS не найден в {grass_base}")
    sys.exit(1)

# Добавление системной переменной в среду
os.environ['GISBASE'] = grass_base

# Присоединение каталогов (путей)
grass_bin = os.path.join(grass_base, "bin")
grass_lib = os.path.join(grass_base, "lib")
grass_scripts = os.path.join(grass_base, "scripts")

os.environ['PATH'] = ";".join([
    grass_bin, grass_lib, grass_scripts,
    os.environ.get('PATH', '')
])

grass_python_paths = [
    os.path.join(grass_base, "etc", "python"),
    os.path.join(grass_base, "gui", "wxpython"),
]
for p in grass_python_paths:
    if os.path.exists(p) and p not in sys.path:
        sys.path.insert(0, p)

os.environ['PYTHONPATH'] = os.path.join(grass_base, "etc", "python") + ";" + os.environ.get('PYTHONPATH', '')
os.environ['GRASSBIN'] = os.path.join(grass_base, "grass78.bat")
os.environ['GRASS_PYTHON'] = sys.executable
os.environ['GRASS_SH'] = os.path.join(grass_base, "msys", "bin", "sh.exe")

# Запуск сессии GRASS GIS
try:
    import grass.script as gs
    import grass.script.setup as gsetup

    print("✓ grass.script импортирован")
except ImportError as e:
    print(f"✗ Ошибка импорта grass.script: {e}")
    sys.exit(1)

try:
    from grass_session import Session

    print("✓ grass_session импортирован")
except ImportError:
    print("✗ grass_session не установлен!")
    sys.exit(1)

# ===== ПУТИ К ДАННЫМ =====
GRASS_DB = r"C:\GRASS\grassdata" # база данных GRASS
LOCATION = "glacier_TEST" # локация GRASS
MAPSET = "PERMANENT" # набор карт GRASS

os.makedirs(GRASS_DB, exist_ok=True)

# ---------------------------------------------------
# ========== CONFIG - постоянные переменные =========
# ---------------------------------------------------
CONFIG = {
    "dem_tif": "DEM.tif",                   # маска ледника
    "elevation_tif": "elevation.tif",       # высота
    "slope_tif": "slope.tif",               # уклон
    "aspect_tif": "aspect.tif",             # направление уклона (азимут)
    "glacier_shp": "glacier.shp",           # shape-файл ледника
    "output_dir": "output_model",           # выходная модель (директория)
    "time_step_minutes": 30,                # шаг вычислений
    "period_start": "2019-07-07T00:00:00",  # начальный период времени
    "period_end": "2019-07-10T23:30:00",    # конечный период времени

    # r.sun параметры
    "linke_value": 3.0,                     # коэффициент Линке = 3
    "albedo_value": 0.2,                    # альбедо для расчета r.sun = 0.2

    # Физические константы
    "kt": -0.0065,                          # вертикальный градиент температуры воздуха
    "asl": 1.7813,                          # коэффициент линейной регрессии в формуле расчета высоты снеговой линии
    "bsl": 2067.6,                          # коэффициент линейной регрессии в формуле расчета высоты снеговой линии
    "kSS": 0.33745,                         # коэффициент линейной регрессии в формуле расчета альбедо α
    "kT2m": 0.00838,                        # коэффициент линейной регрессии в формуле расчета альбедо α
    "kTa": -0.00112,                        # коэффициент линейной регрессии в формуле расчета альбедо α
    "c_alpha": 0.13469,                     # коэффициент линейной регрессии в формуле расчета альбедо α
    "rho_ice": 784,                         # средняя плотность льда
    "rho_snow": 602,                        # средняя плотность снега
    "sigma": 5.670374419e-8,                # постоянная Стефана-Больцмана
    "epsilon": 1,                           # излучательная способность поверхности
    "z_aws1": 2536,                         # высота метеостанции на морене
    "z_aws2": 2549,                         # высота метеостанции на леднике
    "L_fs": 330000,                         # скрытая теплота плавления снега
    "L_fi": 335000,                         # скрытая теплота плавления льда
    "latitude": 56.82,                      # широта
    "longitude": 117.33,                    # долгота
    "timezone": 9                           # часовой пояс
}


def ensure_dir(d):
    os.makedirs(d, exist_ok=True)

# Алгоритм определения солнечного времени для расчета радиации r.sun
def get_solar_time_for_rsun(datetime_obj, longitude, timezone_offset):

    day_of_year = datetime_obj.timetuple().tm_yday
    hour_local = datetime_obj.hour + datetime_obj.minute / 60.0

    # Уравнение времени (в минутах)
    B = 360.0 / 365.0 * (day_of_year - 81)
    B_rad = math.radians(B)
    EoT = (9.87 * math.sin(2 * B_rad)
           - 7.53 * math.cos(B_rad)
           - 1.5 * math.sin(B_rad))

    # Стандартный меридиан для часового пояса (UTC+9 → 135°)
    standard_meridian = 15.0 * timezone_offset  # 135° для UTC+9

    # Поправка на долготу: 4 минуты на каждый градус
    # Если longitude < standard_meridian, солнце приходит ПОЗЖЕ
    longitude_correction = 4.0 * (longitude - standard_meridian)  # минуты

    # Солнечное время
    solar_time = hour_local + (EoT + longitude_correction) / 60.0

    return solar_time

# Вычисление карт горизонта с использованием GRASS
def prepare_horizon_maps():

    print("Вычисляем карты горизонта (это может занять время)...")

    try:
        gs.run_command(
            'r.horizon',
            elevation='DEM',
            output='horizon',
            step=30,  # шаг 30° (12 направлений)
            overwrite=True,
            quiet=True
        )
        print("✓ Карты горизонта созданы")
        return True
    except Exception as e:
        print(f"⚠ Ошибка создания horizon: {e}")
        return False

# Расчет r.sun с использованием GRASS
def run_rsun_for_timestep(day_of_year, local_time, output_suffix, use_horizon=False):

    if local_time < 0 or local_time >= 24:
        return None, None

    glob_name = f"glob_{output_suffix}"

    try:
        params = {
            'elevation': 'DEM',
            'aspect': 'aspect',
            'slope': 'slope',
            'day': day_of_year,
            'time': local_time,
            'glob_rad': glob_name,
            'linke_value': 3.0,
            'albedo_value': 0.2,
            'overwrite': True,
            'quiet': True
        }

        if use_horizon:
            params['horizon_basename'] = 'horizon'
            params['horizon_step'] = 30

        gs.run_command('r.sun', **params)

        return glob_name, [glob_name]

    except Exception as e:
        print(f"⚠ r.sun ошибка: {e}")
        return None, None

# Алгоритм извлечения растровых значений по точкам с использованием GRASS
def extract_raster_values_at_points(raster_name, points_gdf):
    coords = []
    cats = []

    for _, row in points_gdf.iterrows():
        coords.append(f"{row['x']},{row['y']}")
        cats.append(int(row['cat']))

    # ВАЖНО: одна строка x1,y1,x2,y2,...
    flat_coords = []
    for c in coords:
        x, y = c.split(",")
        flat_coords.extend([x, y])

    query = ",".join(flat_coords)

    result = gs.read_command(
        'r.what',
        map=raster_name,
        coordinates=query,
        separator='|',
        quiet=True
    )

    G_values = {}

    lines = result.strip().split('\n')

    for i, line in enumerate(lines):
        parts = line.split('|')
        if len(parts) >= 4:
            try:
                val = parts[3].strip()
                if val and val.upper() != 'NULL':
                    G_values[cats[i]] = float(val)
                else:
                    G_values[cats[i]] = 0.0
            except:
                G_values[cats[i]] = 0.0

    return G_values

# Очистка временных растров
def cleanup_temp_rasters(raster_list):
    """Удаляет временные растры"""
    for name in raster_list:
        try:
            gs.run_command('g.remove', type='raster',
                           name=name, flags='f', quiet=True)
        except:
            pass

# Расчет приходящей радиации по ячейкам
def compute_Sin_cell(Sin_AWS2, G_cell, G_AWS2):
    if G_cell <= 0 or Sin_AWS2 <= 0:
        return 0.0

    if G_AWS2 <= 0:
        return 0.0

    return Sin_AWS2 * (G_cell / G_AWS2)

# Алгоритм определения дня со снегопадом
def calculate_sd(albedo_df, current_date, alpha_d=0.06):
    """
    Рассчитывает SD (день со снегопадом) для заданной даты

    SD(d) = 1 если [α(12h,d) - α(12h,d-1)] >= α_d (0.06)
    SD(d) = 0 если разность меньше α_d
    """
    # Приводим current_date к началу дня (без времени)
    current_day = current_date.replace(hour=0, minute=0, second=0, microsecond=0)

    # Проверка на пустой DataFrame
    if albedo_df.empty:
        print(f"  ⚠ albedo_df пуст, SD = 0 для {current_day.date()}")
        return 0

    # Ищем альбедо для текущего дня в 12 часов
    current_mask = albedo_df['date'].dt.date == current_day.date()
    current_albedo = albedo_df[current_mask]['albedo_12h']

    # Ищем альбедо для предыдущего дня
    prev_day = current_day - dt.timedelta(days=1)
    prev_mask = albedo_df['date'].dt.date == prev_day.date()
    prev_albedo = albedo_df[prev_mask]['albedo_12h']

    # Если данных нет для текущего или предыдущего дня
    if len(current_albedo) == 0:
        print(f"  ⚠ Нет данных альбедо для {current_day.date()}, SD = 0")
        return 0

    alpha_current = float(current_albedo.iloc[0])

    if len(prev_albedo) == 0:
        print(f"  ⚠ Нет данных альбедо для {prev_day.date()}, SD = 0 (первый день моделирования)")
        print(f"     {current_day.date()}: α_curr={alpha_current:.3f}, нет данных за предыдущий день")
        return 0

    alpha_prev = float(prev_albedo.iloc[0])
    alpha_diff = alpha_current - alpha_prev

    # Формула из документации
    if alpha_diff >= alpha_d:
        print(
            f"  {current_day.date()}: α_curr={alpha_current:.3f}, α_prev={alpha_prev:.3f}, diff={alpha_diff:.3f} >= {alpha_d} → SD=1 (снегопад)")
        return 1
    else:
        print(
            f"  {current_day.date()}: α_curr={alpha_current:.3f}, α_prev={alpha_prev:.3f}, diff={alpha_diff:.3f} < {alpha_d} → SD=0 (нет снегопада)")
        return 0

# Расчет высоты снеговой линии (по формуле регрессии)
def calculate_zsl(current_date, asl, bsl):
    # Получаем порядковый день года (1-365)
    day_of_year = current_date.timetuple().tm_yday

    # Рассчитываем Z_sl по формуле линейной регрессии
    zsl = asl * day_of_year + bsl

    return zsl

def compute_T2m_at_z(T2m_aws2, kt, z_cell, z_aws2):
    """Температура воздуха на высоте"""
    return T2m_aws2 + kt * (z_cell - z_aws2)


def compute_albedo(ST, T2m, Ta, k_ST, k_T2m, k_Ta, c_alpha):
    """Альбедо поверхности"""
    albedo = k_ST * ST + k_T2m * T2m + k_Ta * Ta + c_alpha
    return max(0.1, min(0.9, albedo))


def compute_Sout(alpha, Sin):
    """Отражённая радиация"""
    return alpha * Sin


def compute_Lout(epsilon, sigma, ST, Qm):
    """Длинноволновое излучение поверхности"""
    if Qm > 0:
        Ts_K = 273.15  # таяние — 0°C
    else:
        Ts_K = 271.15 if ST == 1 else 272.15

    Lout = epsilon * sigma * (Ts_K ** 4)
    return Lout, Ts_K - 273.15


def compute_Rnet(Sin, Sout, Lin, Lout):
    """Радиационный баланс"""
    Snet = Sin - Sout
    Lnet = Lin - Lout
    return Snet + Lnet, Snet, Lnet


def compute_turbulent_heat(T2m_pt, Ts_C, wind_speed, pressure, RH, z,
                           z0m=0.001, z0t=0.0001, z0h=0.0001, zm=2.0):
    """Явный и латентный теплообмен"""
    cp = 1005.0
    rho0 = 1.225
    p0 = 1013.25
    k = 0.4
    Lv = 2.83e6
    e_s = 6.11

    if wind_speed <= 0.3:
        return 0.0, 0.0

    T2m_K = T2m_pt + 273.15
    delta_T = T2m_pt - Ts_C

    # Число Ричардсона
    Rib = (9.81 * delta_T * (zm - z0m)) / (T2m_K * wind_speed ** 2) if wind_speed > 0 else 0.0

    if Rib >= 0.2:
        return 0.0, 0.0

    # Функция устойчивости
    if Rib > 0:
        phi_inv = (1.0 - 5.0 * Rib) ** 2
    else:
        phi_inv = (1.0 - 16.0 * Rib) ** 0.75

    ln_m = math.log(zm / z0m)
    ln_t = math.log(zm / z0t)
    ln_h = math.log(zm / z0h)

    # H
    H = cp * rho0 * (pressure / p0) * (k ** 2) * wind_speed * delta_T * phi_inv / (ln_m * ln_t)

    # LE
    if T2m_pt < -80 or T2m_pt > 60:
        e_air = 0.0
    else:
        term1 = 6.112 * math.exp(17.62 * T2m_pt / (243.12 + T2m_pt))
        term2 = 1.0016 + 0.0000315 * pressure - 0.074 / pressure if pressure > 0 else 1.0
        e_air = term1 * term2 * (RH / 100.0)

    delta_e = e_air - e_s
    LE = 0.623 * Lv * rho0 * (1.0 / p0) * (k ** 2) * wind_speed * delta_e * phi_inv / (ln_m * ln_h)

    return H, LE


def compute_rain_heat(T2m_pt, Ts_C, precipitation_rate):
    """Теплота дождя"""
    if T2m_pt < 2.0 or precipitation_rate <= 0:
        return 0.0

    rho_water = 1000.0
    cp_water = 4186.0
    precip_ms = precipitation_rate / 3600.0 / 1000.0
    return rho_water * cp_water * precip_ms * (T2m_pt - Ts_C)


def compute_ground_heat(ST, T_surface_C, k_r_snow=0.2, k_r_ice=2.2, z_g=0.1, z_0=0.01):
    """Теплопоток в грунт"""
    k_r = k_r_snow if ST == 1 else k_r_ice
    T_g_K = 271.15 if ST == 1 else 272.15
    T_s_K = T_surface_C + 273.15
    return -k_r * (T_g_K - T_s_K) / (z_g - z_0)


def compute_melting_heat(Sin, Sout, Lin, Lout, H, LE, Qr, Qg):
    """Энергия таяния"""
    Qm = (Sin - Sout) + (Lin - Lout) + H + LE + Qr + Qg
    return max(0.0, Qm)


def compute_ablation(Qm, ST, time_step_seconds, rho_snow, rho_ice, L_fs, L_fi):
    """Абляция в мм в.э."""
    if Qm <= 0:
        return 0.0

    L_f = L_fs if ST == 1 else L_fi
    melting_energy = Qm * time_step_seconds
    melted_mass = melting_energy / L_f
    return (melted_mass / 1000.0) * 1000.0

#  СОЗДАНИЕ ТОЧЕК

def create_research_points(dem_tif, glacier_shp, num_points=100):
    """Создаёт точки на леднике"""
    print(f"Создаём точки (цель: {num_points})...")

    POINT_94_X, POINT_94_Y = 525285, 6300765
    AWS2_X, AWS2_Y = 525465, 6300765

    with rasterio.open(dem_tif) as src:
        glacier_gdf = gpd.read_file(glacier_shp)
        if glacier_gdf.crs != src.crs:
            glacier_gdf = glacier_gdf.to_crs(src.crs)

        points = []

        # Точка 94
        try:
            row_94, col_94 = src.index(POINT_94_X, POINT_94_Y)
            z_94 = src.read(1, window=rasterio.windows.Window(col_94, row_94, 1, 1))[0, 0]
            points.append({
                'cat': 94, 'x': POINT_94_X, 'y': POINT_94_Y, 'z': z_94,
                'row': row_94, 'col': col_94,
                'geometry': gpd.points_from_xy([POINT_94_X], [POINT_94_Y])[0]
            })
            print(f"  ✓ Точка 94: Z={z_94:.1f}")
        except Exception as e:
            print(f"  ✗ Точка 94: {e}")

        # Точка AWS2 (cat=96)
        try:
            row_aws2, col_aws2 = src.index(AWS2_X, AWS2_Y)
            z_aws2 = src.read(1, window=rasterio.windows.Window(col_aws2, row_aws2, 1, 1))[0, 0]
            points.append({
                'cat': 96, 'x': AWS2_X, 'y': AWS2_Y, 'z': z_aws2,
                'row': row_aws2, 'col': col_aws2,
                'geometry': gpd.points_from_xy([AWS2_X], [AWS2_Y])[0]
            })
            print(f"  ✓ Точка AWS2 (96): Z={z_aws2:.1f}")
        except Exception as e:
            print(f"  ✗ Точка AWS2: {e}")

        # Остальные точки
        cat_counter = 1
        for j in range(src.height):
            for i in range(src.width):
                while cat_counter in [94, 96]:
                    cat_counter += 1
                if len(points) >= num_points:
                    break

                x, y = src.xy(j, i)
                point_geom = gpd.points_from_xy([x], [y])[0]

                if glacier_gdf.contains(point_geom).any():
                    z = src.read(1, window=rasterio.windows.Window(i, j, 1, 1))[0, 0]
                    if not np.isnan(z) and z > -9999:
                        points.append({
                            'cat': cat_counter, 'x': x, 'y': y, 'z': z,
                            'row': j, 'col': i, 'geometry': point_geom
                        })
                        cat_counter += 1
            if len(points) >= num_points:
                break

        points_gdf = gpd.GeoDataFrame(points, crs=src.crs)
        print(f"✓ Всего точек: {len(points_gdf)}")
        return points_gdf

#  ЗАГРУЗКА МЕТЕОДАННЫХ

def load_aws_data(excel_file="Test_model.xlsx", sheet_name="AWS2_30min"):
    """Загружает метеоданные"""
    try:
        print(f"Загружаем метеоданные из {excel_file}...")
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=2)

        column_mapping = {
            'Sin': 'Sin_AWS2', 'Sout': 'Sout_AWS2', 'Lin': 'Lin_AWS2',
            'T2m': 'T2m_AWS2', 'RH2m': 'RH_AWS2', 'W2m': 'wind_speed',
            'p': 'pressure', 'Prec': 'precipitation', 'α': 'alpha_AWS2'
        }
        df = df.rename(columns=column_mapping)

        if 'Дата&Время' in df.columns:
            df['datetime'] = pd.to_datetime(df['Дата&Время'])

        df = df.dropna(subset=['datetime']).sort_values('datetime').reset_index(drop=True)
        print(f"✓ Загружено {len(df)} записей")
        return df
    except Exception as e:
        print(f"✗ Ошибка: {e}")
        return pd.DataFrame()

def get_aws_at_time(aws_df, target_datetime):
    """Получает метеоданные для времени"""

    def safe_float(value, default=0.0):
        try:
            if pd.isna(value):
                return default
            return float(value)
        except:
            return default

    mask = aws_df['datetime'] == target_datetime
    if mask.any():
        row = aws_df[mask].iloc[0]
        return {
            'Sin_AWS2': safe_float(row.get('Sin_AWS2')),
            'Sout_AWS2': safe_float(row.get('Sout_AWS2')),
            'Lin_AWS2': safe_float(row.get('Lin_AWS2'), 300.0),
            'T2m_AWS2': safe_float(row.get('T2m_AWS2')),
            'RH_AWS2': safe_float(row.get('RH_AWS2'), 70.0),
            'wind_speed': safe_float(row.get('wind_speed'), 2.0),
            'pressure': safe_float(row.get('pressure'), 750.0),
            'precipitation': safe_float(row.get('precipitation')),
            'alpha_AWS2': safe_float(row.get('alpha_AWS2'), 0.5),
        }

    # Интерполяция
    before = aws_df[aws_df['datetime'] <= target_datetime]
    after = aws_df[aws_df['datetime'] >= target_datetime]

    if len(before) > 0 and len(after) > 0:
        rb, ra = before.iloc[-1], after.iloc[0]
        tb, ta = rb['datetime'], ra['datetime']
        w = ((target_datetime - tb).total_seconds() / (ta - tb).total_seconds()) if ta != tb else 0.5

        def interp(col, default=0.0):
            return safe_float(rb.get(col), default) * (1 - w) + safe_float(ra.get(col), default) * w

        return {
            'Sin_AWS2': interp('Sin_AWS2'),
            'Sout_AWS2': interp('Sout_AWS2'),
            'Lin_AWS2': interp('Lin_AWS2', 300.0),
            'T2m_AWS2': interp('T2m_AWS2'),
            'RH_AWS2': interp('RH_AWS2', 70.0),
            'wind_speed': interp('wind_speed', 2.0),
            'pressure': interp('pressure', 750.0),
            'precipitation': interp('precipitation'),
            'alpha_AWS2': interp('alpha_AWS2', 0.5),
        }

    return {
        'Sin_AWS2': 0.0, 'Sout_AWS2': 0.0, 'Lin_AWS2': 300.0,
        'T2m_AWS2': 5.0, 'RH_AWS2': 70.0, 'wind_speed': 2.0,
        'pressure': 750.0, 'precipitation': 0.0, 'alpha_AWS2': 0.5
    }


def load_albedo_from_excel(excel_file="Test_model.xlsx", sheet_name="Albedo"):
    """
    Загружает альбедо в 12 часов для расчета SD (день со снегопадом)
    Из листа Albedo, начиная с ячейки D3
    """
    try:
        print(f"Загружаем альбедо (12h) из {excel_file}, лист '{sheet_name}'...")

        # Читаем файл, пропускаем первые 2 строки (заголовки)
        # header=None чтобы прочитать все данные как есть
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None, skiprows=2)

        print(f"  Формат файла: {df.shape[0]} строк, {df.shape[1]} колонок")
        print(f"  Первые 5 строк:")
        for i in range(min(5, len(df))):
            print(f"    Строка {i}: {df.iloc[i, :3].values} ...")

        # По описанию: колонка A (индекс 0) - дата, колонка D (индекс 3) - альбедо в 12h
        # Но в вашем выводе видно, что альбедо находится в колонке с индексом 3

        # Создаем DataFrame с правильными колонками
        df_albedo = pd.DataFrame()

        # Дата - первая колонка (индекс 0)
        df_albedo['date'] = df.iloc[:, 0]

        # Альбедо в 12h - четвертая колонка (индекс 3)
        df_albedo['albedo_12h'] = df.iloc[:, 3]

        # Преобразуем дату в datetime
        df_albedo['date'] = pd.to_datetime(df_albedo['date'], errors='coerce')

        # Преобразуем альбедо в числа (удаляем возможные текстовые значения)
        df_albedo['albedo_12h'] = pd.to_numeric(df_albedo['albedo_12h'], errors='coerce')

        # Удаляем строки с некорректной датой или NaN
        initial_len = len(df_albedo)
        df_albedo = df_albedo.dropna(subset=['date', 'albedo_12h'])

        # Также удаляем строки, где альбедо > 1 (это явно не альбедо, а какие-то другие данные)
        df_albedo = df_albedo[df_albedo['albedo_12h'] <= 1.0]

        print(f"  Удалено {initial_len - len(df_albedo)} строк с некорректными данными")

        # Сортируем по дате
        df_albedo = df_albedo.sort_values('date').reset_index(drop=True)

        print(f"✓ Загружено {len(df_albedo)} записей альбедо в 12h")
        if len(df_albedo) > 0:
            print(f"  Диапазон дат: {df_albedo['date'].min().date()} - {df_albedo['date'].max().date()}")
            print(f"  Первые 5 записей:")
            for i in range(min(5, len(df_albedo))):
                print(f"    {df_albedo.iloc[i]['date'].date()}: {df_albedo.iloc[i]['albedo_12h']:.3f}")

        return df_albedo

    except Exception as e:
        print(f"✗ Ошибка загрузки альбедо: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def compute_daily_mean_temperatures(aws_df):

    # Создаем копию, чтобы не изменять исходный DataFrame
    df_temp = aws_df.copy()
    # Извлекаем дату из datetime
    df_temp['date'] = df_temp['datetime'].dt.date
    # Группируем по дате и вычисляем среднюю температуру
    daily_mean = df_temp.groupby('date')['T2m_AWS2'].mean().to_dict()
    return daily_mean

#  ГЛАВНАЯ ФУНКЦИЯ

def run_glacier_model(config=CONFIG):
    print("=" * 60)
    print("МОДЕЛЬ ТАЯНИЯ ЛЕДНИКА (ИСПРАВЛЕННАЯ)")
    print("=" * 60)

    ensure_dir(config["output_dir"])

    # 1. Метеоданные
    aws_df = load_aws_data()
    if aws_df.empty:
        print("✗ Нет метеоданных!")
        return

    # Вычисляем среднесуточные температуры на AWS2
    daily_mean_T2m = compute_daily_mean_temperatures(aws_df)
    print(f"✓ Загружены среднесуточные температуры для {len(daily_mean_T2m)} дней")

    albedo_df = load_albedo_from_excel()
    if albedo_df.empty:
        print("⚠ Внимание: не удалось загрузить альбедо для расчета SD!")
        print("  SD будет всегда = 0")
    else:
        # Выводим все даты и значения для проверки
        print("\n=== ДАННЫЕ АЛЬБЕДО (12h) ===")
        for idx, row in albedo_df.iterrows():
            print(f"  {row['date'].date()}: {row['albedo_12h']:.3f}")

    # 2. Точки
    points_gdf = create_research_points(config["dem_tif"], config["glacier_shp"])
    if points_gdf.empty:
        raise Exception("Не удалось создать точки!")

    # 3. Временной диапазон
    start = pd.to_datetime(config["period_start"])
    end = pd.to_datetime(config["period_end"])
    step_min = config["time_step_minutes"]
    step_sec = step_min * 60
    all_times = pd.date_range(start, end, freq=f'{step_min}min')

    print(f"\nРасчёт: {len(points_gdf)} точек × {len(all_times)} шагов")

    results = []
    aws2_cat = 96

    # 4. GRASS сессия
    with Session(gisdb=GRASS_DB, location=LOCATION, mapset=MAPSET) as sess:
        print("✓ GRASS session started")

        gs.run_command('g.region', raster='DEM', quiet=True)

        # Slope/aspect
        if not gs.find_file('slope', element='cell')['file']:
            print("Вычисляем slope и aspect...")
            gs.run_command('r.slope.aspect', elevation='DEM',
                           slope='slope', aspect='aspect', overwrite=True)

        horizon_exists = gs.find_file('horizon_000', element='cell')['file']
        if not horizon_exists:
            use_horizon = prepare_horizon_maps()
        else:
            print("✓ Карты горизонта уже существуют")
            use_horizon = True

        # Импорт точек
        tmp_shp = os.path.join(tempfile.gettempdir(), "research_points.shp")
        points_gdf.to_file(tmp_shp)
        gs.run_command('v.in.ogr', input=tmp_shp, output='points',
                       overwrite=True, flags='o', quiet=True)
        gs.run_command('v.db.addcolumn', map='points',
                       columns='G double precision', quiet=True)

        print("✓ Данные подготовлены")

        # ========================================
        #  ГЛАВНЫЙ ЦИКЛ
        # ========================================
        prev_day = -1
        daily_T2m_per_point = {}
        daily_Ta_per_point = {}
        sd = 0  # начальное значение SD
        zsl = config["bsl"]  # начальное значение Z_sl
        nd_aws2 = 0  # число дней после последнего снегопада на AWS2
        ta_aws2 = 0.0  # сумма температур на AWS2 после последнего снегопада

        for step_i, current_time in enumerate(all_times):
            day_of_year = current_time.timetuple().tm_yday

            # Вычисляем SD для текущего дня (один раз в начале дня)
            # SD одинаков для всех ячеек в данный день
            if current_time.hour == 0 and current_time.minute == 0:
                sd = calculate_sd(albedo_df, current_time, alpha_d=0.06)
                zsl = calculate_zsl(current_time, config["asl"], config["bsl"])

                current_date = current_time.date()
                t2m_mean_today = daily_mean_T2m.get(current_date, 0.0)

                if sd == 1:
                    nd_aws2 = 0
                    ta_aws2 = 0.0
                else:
                    nd_aws2 += 1
                    ta_aws2 += t2m_mean_today  # сумма средних суточных T на AWS2

                # Словарь: T2m и Ta для каждой точки на текущий день
                daily_T2m_per_point = {}
                daily_Ta_per_point = {}
                for _, pt in points_gdf.iterrows():
                    z_pt = pt['z']
                    dz = z_pt - config["z_aws2"]
                    # Средняя суточная T в ячейке
                    T2m_daily = t2m_mean_today + config["kt"] * dz
                    # Сумма температур со дня снегопада в ячейке
                    Ta_daily = ta_aws2 + (nd_aws2 + 1) * config["kt"] * dz
                    daily_T2m_per_point[int(pt['cat'])] = T2m_daily
                    daily_Ta_per_point[int(pt['cat'])] = Ta_daily

                print(f"  SD={sd}, Z_sl={zsl:.1f} м, nd_aws2={nd_aws2}, "
                      f"Ta_aws2={ta_aws2:.2f} °C·дней, T2m_AWS2_mean={t2m_mean_today:.2f}")

            # SD сохраняется на весь день

            if current_time.day != prev_day:
                prev_day = current_time.day
                print(f"\n--- {current_time.strftime('%Y-%m-%d')} ({step_i + 1}/{len(all_times)}) ---")

            solar_time = current_time.hour + current_time.minute / 60.0

            # Метеоданные
            aws_data = get_aws_at_time(aws_df, current_time)

            # r.sun
            G_values = {}
            rasters_to_cleanup = []

            if 0 < solar_time < 24:
                output_suffix = f"d{day_of_year}_t{current_time.strftime('%H%M')}"
                glob_map, temp_rasters = run_rsun_for_timestep(
                    day_of_year, solar_time, output_suffix,
                    use_horizon=False
                )

                if glob_map:
                    G_values = extract_raster_values_at_points(glob_map, points_gdf)
                    rasters_to_cleanup = temp_rasters or []

            G_AWS2 = G_values.get(aws2_cat, 0.0)

            # Для каждой точки
            for idx, point in points_gdf.iterrows():
                cat = int(point['cat'])
                z = point['z']
                G_cell = G_values.get(cat, 0.0)

                # Sin
                Sin_cell = compute_Sin_cell(aws_data['Sin_AWS2'], G_cell, G_AWS2)

                # Температура
                T2m_pt  = daily_T2m_per_point.get(cat, 0.0)   # средняя суточная T в ячейке

                # Тип поверхности
                if sd == 1 or z >= zsl:
                    ST = 1  # снег
                else:
                    ST = 0  # лед

                Ta_cell = daily_Ta_per_point.get(cat, 0.0)

                # Альбедо (теперь используем Ta_cell вместо константы 50)
                alpha = compute_albedo(ST, T2m_pt, Ta_cell, config["kSS"],
                                       config["kT2m"], config["kTa"], config["c_alpha"])

                # Sout
                Sout = compute_Sout(alpha, Sin_cell)

                # Lin
                Lin = aws_data['Lin_AWS2']

                # Итерация 1
                Lout_1, Ts_1 = compute_Lout(config["epsilon"], config["sigma"], ST, 0)
                H_1, LE_1 = compute_turbulent_heat(T2m_pt, Ts_1, aws_data['wind_speed'],
                                                   aws_data['pressure'], aws_data['RH_AWS2'], z)
                Qr_1 = compute_rain_heat(T2m_pt, Ts_1, aws_data['precipitation'])
                Qg_1 = compute_ground_heat(ST, Ts_1)
                Qm_1 = compute_melting_heat(Sin_cell, Sout, Lin, Lout_1, H_1, LE_1, Qr_1, Qg_1)

                # Итерация 2
                Lout, Ts = compute_Lout(config["epsilon"], config["sigma"], ST, Qm_1)
                H, LE = compute_turbulent_heat(T2m_pt, Ts, aws_data['wind_speed'],
                                               aws_data['pressure'], aws_data['RH_AWS2'], z)
                Qr = compute_rain_heat(T2m_pt, Ts, aws_data['precipitation'])
                Qg = compute_ground_heat(ST, Ts)
                Rnet, Snet, Lnet = compute_Rnet(Sin_cell, Sout, Lin, Lout)
                Qm = compute_melting_heat(Sin_cell, Sout, Lin, Lout, H, LE, Qr, Qg)
                ablation = compute_ablation(Qm, ST, step_sec, config["rho_snow"],
                                            config["rho_ice"], config["L_fs"], config["L_fi"])

                results.append({
                    'datetime': current_time,
                    'day_of_year': day_of_year,
                    'solar_time': round(solar_time, 2),
                    'cat': cat,
                    'z': z,
                    'ST': ST,
                    'SD': sd,
                    'Z_sl': round(zsl, 1),
                    'nd_aws2': nd_aws2,  # число дней после снегопада на AWS2
                    'Ta_aws2': round(ta_aws2, 2),  # сумма температур на AWS2
                    'Ta_cell': round(Ta_cell, 2),  # сумма температур в ячейке
                    'G_rsun': round(G_cell, 2),
                    'G_AWS2_rsun': round(G_AWS2, 2),
                    'Sin_AWS2': round(aws_data['Sin_AWS2'], 2),
                    'Sin_cell': round(Sin_cell, 2),
                    'alpha': round(alpha, 4),
                    'Sout': round(Sout, 2),
                    'Lin': round(Lin, 2),
                    'Lout': round(Lout, 2),
                    'Snet': round(Snet, 2),
                    'Lnet': round(Lnet, 2),
                    'Rnet': round(Rnet, 2),
                    'T2m_AWS2': round(aws_data['T2m_AWS2'], 2),
                    'T2m': round(T2m_pt, 2),
                    'Ts': round(Ts, 2),
                    'wind_speed': round(aws_data['wind_speed'], 2),
                    'RH': round(aws_data['RH_AWS2'], 2),
                    'pressure': round(aws_data['pressure'], 2),
                    'H': round(H, 2),
                    'LE': round(LE, 2),
                    'Qr': round(Qr, 2),
                    'Qg': round(Qg, 2),
                    'Qm': round(Qm, 2),
                    'ablation_mm': round(ablation, 4),
                })

            # Очистка
            if rasters_to_cleanup:
                cleanup_temp_rasters(rasters_to_cleanup)

    #  СОХРАНЕНИЕ

    print("\n" + "=" * 60)
    print("СОХРАНЕНИЕ")
    print("=" * 60)

    results_df = pd.DataFrame(results)

    if results_df.empty:
        print("⚠ Результаты пусты!")
        return

    out_csv = Path(config["output_dir"]) / "model_results.csv"
    results_df.to_csv(out_csv, index=False)
    print(f"✓ CSV: {out_csv}")

    out_xlsx = Path(config["output_dir"]) / "model_results.xlsx"
    with pd.ExcelWriter(out_xlsx, engine='openpyxl') as writer:
        results_df.to_excel(writer, sheet_name='model_30min', index=False)

        daily = results_df.groupby([results_df['datetime'].dt.date, 'cat']).agg({
            'Sin_cell': 'sum', 'Qm': 'sum', 'ablation_mm': 'sum', 'z': 'first', 'T2m': 'mean'
        }).reset_index()
        daily.to_excel(writer, sheet_name='daily_summary', index=False)

        point_summary = results_df.groupby('cat').agg({
            'ablation_mm': 'sum', 'Qm': 'mean', 'z': 'first', 'T2m': 'mean'
        }).reset_index()
        point_summary.to_excel(writer, sheet_name='point_summary', index=False)

    print(f"✓ Excel: {out_xlsx}")

    # Статистика по точке 94
    p94_data = results_df[results_df['cat'] == 94].sort_values('datetime')
    if not p94_data.empty:
        print(f"\n--- Точка 94, G_rsun по времени ---")
        for _, row in p94_data.iterrows():
            print(f"{row['datetime'].strftime('%H:%M')}: {row['G_rsun']:.2f}")

    print(f"\n--- СТАТИСТИКА ---")
    print(f"Sin_cell: min={results_df['Sin_cell'].min():.1f}, max={results_df['Sin_cell'].max():.1f}")
    print(f"Qm: min={results_df['Qm'].min():.1f}, max={results_df['Qm'].max():.1f}")
    print(f"Абляция: {results_df['ablation_mm'].sum():.2f} мм")
    print("\n✓ ГОТОВО!")

# ЗАПУСК МОДЕЛИ
if __name__ == "__main__":
    run_glacier_model()