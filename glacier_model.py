#!/usr/bin/env python3

import os
import sys
import math
import tempfile
import datetime as dt
from pathlib import Path
import shutil
import subprocess
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.crs import CRS

# ===== НАСТРОЙКА GRASS GIS =====
grass_base = r"C:\GRASS"

if not os.path.exists(grass_base):
    print(f"✗ GRASS не найден в {grass_base}")
    sys.exit(1)

os.environ['GISBASE'] = grass_base

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

grass_pythonpath = os.path.join(grass_base, "etc", "python")
existing_pythonpath = os.environ.get('PYTHONPATH', '')
os.environ['PYTHONPATH'] = grass_pythonpath + ";" + existing_pythonpath

os.environ['GRASSBIN'] = os.path.join(grass_base, "grass78.bat")
os.environ['GRASS_PYTHON'] = sys.executable
os.environ['GRASS_SH'] = os.path.join(grass_base, "msys", "bin", "sh.exe")

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
GRASS_DB = r"C:\GRASS\grassdata"
LOCATION = "glacier_TEST"
MAPSET = "PERMANENT"

if not os.path.exists(GRASS_DB):
    os.makedirs(GRASS_DB, exist_ok=True)


# ---------------------------
# ========== CONFIG =========
# ---------------------------
CONFIG = {
    "dem_tif": "DEM.tif",
    "elevation_tif": "elevation.tif",
    "slope_tif": "slope.tif",
    "aspect_tif": "aspect.tif",
    "glacier_shp": "glacier.shp",
    "output_dir": "output_model",
    "time_step_minutes": 30,
    "period_start": "2019-07-07T00:00:00",
    "period_end": "2019-07-08T23:30:00",
    "kt": -0.0065,
    "asl": 1.7813,
    "bsl": 2067.6,
    "kSS": 0.33745,
    "kT2m": 0.00838,
    "kTa": -0.00112,
    "c_alpha": 0.13469,
    "rho_ice": 784,
    "rho_snow": 602,
    "sigma": 5.670374419e-8,
    "epsilon": 1,
    "z_aws1": 2536,
    "z_aws2": 2549,
    "L_fs": 330000,
    "L_fi": 335000,
    "latitude": 56.82,
    "longitude": 117.33,
    "timezone": 9
}


def ensure_dir(d):
    os.makedirs(d, exist_ok=True)


# ================================================================
#  ИСПРАВЛЕНИЕ 1: ПРАВИЛЬНЫЙ ПЕРЕВОД ГРАЖДАНСКОГО ВРЕМЕНИ В
#  СОЛНЕЧНОЕ С УЧЁТОМ ДОЛГОТЫ И УРАВНЕНИЯ ВРЕМЕНИ
# ================================================================
def civil_to_solar_time(datetime_obj, longitude, timezone_offset):
    """
    Перевод гражданского времени в солнечное.

    ИСПРАВЛЕНО: правильная формула EoT
    """
    day_of_year = datetime_obj.timetuple().tm_yday
    hour_decimal = datetime_obj.hour + datetime_obj.minute / 60.0

    # Уравнение времени (упрощённая формула, более точная)
    # Источник: NOAA Solar Calculator
    B = 360.0 / 365.0 * (day_of_year - 81)
    B_rad = math.radians(B)

    EoT_minutes = (9.87 * math.sin(2 * B_rad)
                   - 7.53 * math.cos(B_rad)
                   - 1.5 * math.sin(B_rad))

    # Стандартный меридиан для часового пояса
    standard_meridian = 15.0 * timezone_offset

    # Поправка на долготу (в минутах, потом переводим в часы)
    # 4 минуты на каждый градус долготы
    longitude_correction_minutes = 4.0 * (longitude - standard_meridian)

    # Солнечное время
    solar_time = (hour_decimal
                  + EoT_minutes / 60.0
                  + longitude_correction_minutes / 60.0)

    return solar_time


# ================================================================
#  ИСПРАВЛЕНИЕ 2: r.sun — ПРАВИЛЬНЫЙ ВЫЗОВ
#  Используем r.sun В РЕЖИМЕ mode1 (мгновенная радиация)
#  и затем интегрируем за 30 минут (среднее двух моментов)
# ================================================================
def run_rsun_instantaneous(day_of_year, solar_time, output_name):
    """
    r.sun для ГОРИЗОНТАЛЬНОЙ поверхности.
    """
    # Создаём имена для выходных растров
    beam_name = f"beam_{output_name}"
    diff_name = f"diff_{output_name}"
    glob_name = f"glob_{output_name}"

    try:
        gs.run_command(
            'r.sun',
            # flags='p',
            elevation='DEM',
            slope='slope',
            aspect='aspect',
            day=day_of_year,
            time=solar_time,
            beam_rad=beam_name,  # прямая радиация
            diff_rad=diff_name,  # рассеянная радиация
            linke_value=3.0,
            horizon_basename='horizon',
            horizon_step=5,
            overwrite=True,
            quiet=True
        )
        gs.run_command(
            'r.mapcalc',
            expression=f"{glob_name} = {beam_name} + {diff_name}",
            overwrite=True,
            quiet=True
        )

        return glob_name, [beam_name, diff_name, glob_name]

    except Exception as e:
        print(f"  ⚠ r.sun ошибка для day={day_of_year}, "
              f"time={solar_time:.2f}: {e}")
        return None, None

def extract_raster_at_points(raster_name, points_cats):
    """
    Извлекает значения растра в точках.
    Возвращает dict {cat: value}.
    """
    # Обновляем колонку G
    gs.run_command(
        'v.what.rast',
        map='points',
        raster=raster_name,
        column='G',
        quiet=True
    )

    # Читаем таблицу
    table = gs.read_command(
        'v.db.select',
        map='points',
        columns='cat,G',
        quiet=True
    )

    G_values = {}
    for line in table.strip().split('\n')[1:]:
        if '|' in line:
            parts = line.split('|')
            if len(parts) >= 2:
                try:
                    cat = int(parts[0].strip())
                    val_str = parts[1].strip()
                    if val_str and val_str.upper() not in ('', 'NULL', '*'):
                        G_values[cat] = float(val_str)
                    else:
                        G_values[cat] = 0.0
                except (ValueError, TypeError):
                    pass

    return G_values


def cleanup_temp_rasters(pattern_list):
    """Удаляет временные растры для экономии памяти"""
    for name in pattern_list:
        try:
            gs.run_command('g.remove', type='raster',
                           name=name, flags='f', quiet=True)
        except:
            pass


# ================================================================
#  ИСПРАВЛЕНИЕ 3: ПРАВИЛЬНЫЙ РАСЧЁТ Sin_cell
#  Формула: Sin(z,t) = Sin(AWS2,t) × G(z,t) / G(AWS2,t)
#
#  Проблема была в том, что при малых G_AWS2 получалось деление
#  на ~0 и огромные значения. Нужна корректная обработка.
# ================================================================
def compute_Sin_cell_corrected(Sin_AWS2, G_cell, G_AWS2,
                               min_G_threshold=10.0):
    """
    ПРАВИЛЬНЫЙ расчёт Sin_cell с учётом облачности.

    Логика:
    1. G_AWS2, G_cell — потенциальная радиация при ясном небе (из r.sun)
    2. Sin_AWS2 — реальная измеренная радиация (из метеостанции)
    3. Cloudiness = Sin_AWS2 / G_AWS2 — коэффициент облачности
    4. Sin_cell = G_cell × Cloudiness — реальная радиация в точке

    ВАЖНО: коэффициент облачности может быть >1 (отражения от облаков)
    или <1 (затенение облаками).
    """
    # Ночь
    if G_cell <= 0:
        return 0.0

    # Если нет данных с метеостанции
    if Sin_AWS2 <= 0:
        return 0.0

    # Если G_AWS2 слишком мала (сумерки, r.sun неточен)
    # используем Sin_AWS2 напрямую
    if G_AWS2 < min_G_threshold:
        # В сумерках предполагаем, что облачность одинакова
        # и Sin пропорционален потенциалу
        if G_cell < min_G_threshold:
            return Sin_AWS2  # обе точки в сумерках
        else:
            # AWS2 в сумерках, точка на свету — берём G_cell
            return min(G_cell, Sin_AWS2 * 2)  # с ограничением

    # === ОСНОВНАЯ ФОРМУЛА ===
    # Коэффициент облачности (обычно 0.3-0.9, может быть >1)
    cloudiness = Sin_AWS2 / G_AWS2

    # ОГРАНИЧЕНИЕ: cloudiness обычно в диапазоне 0-1.2
    # >1 возможно из-за отражений от краёв облаков
    # <0.1 — очень плотная облачность или ошибка данных
    cloudiness = max(0.0, min(1.5, cloudiness))

    # Реальная радиация в точке
    sin_cell = G_cell * cloudiness

    # Физический максимум (внеатмосферная радиация)
    MAX_RADIATION = 1400.0
    sin_cell = min(sin_cell, MAX_RADIATION)

    return max(0.0, sin_cell)


# ================================================================
#  ФИЗИЧЕСКИЕ ФОРМУЛЫ (исправленные)
# ================================================================

def compute_T2m_at_z(T2m_aws2, kt, z_cell, z_aws2):
    """
    Температура воздуха на высоте ячейки.
    T2m(z) = T2m(AWS2) + kt × (z - z_AWS2)
    """
    return T2m_aws2 + kt * (z_cell - z_aws2)


def compute_pressure_at_z(p_aws1, z_cell, z_aws1, T_layer_C):
    """
    Давление на высоте ячейки (барометрическая формула).
    p(z) = p(AWS1) / 10^((z - z_AWS1) / (18400 × (1 + 0.003665 × T)))
    """
    denominator = 18400.0 * (1.0 + 0.003665 * T_layer_C)
    if abs(denominator) < 1e-6:
        return p_aws1
    exponent = (z_cell - z_aws1) / denominator
    return p_aws1 / (10.0 ** exponent)


def compute_vapor_pressure(T2m, RH, p):
    """
    Давление водяного пара.
    e = 6.112 × exp(17.62×T/(243.12+T)) ×
        (1.0016 + 3.15e-5×p - 0.074/p) × RH/100
    """
    if T2m < -80 or T2m > 60:
        return 0.0
    term1 = 6.112 * math.exp(17.62 * T2m / (243.12 + T2m))
    if p > 0:
        term2 = 1.0016 + 0.0000315 * p - 0.074 / p
    else:
        term2 = 1.0
    return term1 * term2 * (RH / 100.0)


def compute_albedo(ST, T2m, Ta, k_ST, k_T2m, k_Ta, c_alpha):
    """
    Альбедо поверхности.
    alpha = kSS×ST + kT2m×T2m + kTa×Ta + c_alpha
    """
    albedo = k_ST * ST + k_T2m * T2m + k_Ta * Ta + c_alpha
    return max(0.1, min(0.9, albedo))


def compute_Sout(alpha, Sin):
    """Отражённая коротковолновая радиация: Sout = α × Sin"""
    return alpha * Sin


def compute_Lout(epsilon, sigma, ST, Qm):
    """
    Длинноволновое излучение поверхности.
    Lout = ε × σ × Ts⁴

    При таянии (Qm > 0) температура поверхности = 0°C.
    """
    if Qm > 0:
        Ts_K = 273.15
    else:
        if ST == 1:  # снег
            Ts_K = 271.15  # -2°C
        else:  # лёд
            Ts_K = 272.15  # -1°C

    Lout = epsilon * sigma * (Ts_K ** 4)
    Ts_C = Ts_K - 273.15
    return Lout, Ts_C


def compute_Rnet(Sin, Sout, Lin, Lout):
    """Радиационный баланс"""
    Snet = Sin - Sout
    Lnet = Lin - Lout
    return Snet + Lnet, Snet, Lnet


def compute_dimensionless_functions(Rib):
    """
    Безразмерные функции устойчивости.
    Стабильные (Rib > 0): φ⁻¹ = (1 - 5·Rib)²
    Нестабильные (Rib < 0): φ⁻¹ = (1 - 16·Rib)^0.75
    """
    if Rib > 0:
        if Rib >= 0.2:
            return 0.0  # полностью стабильно, нет турбулентности
        phi_inv = (1.0 - 5.0 * Rib) ** 2
    else:
        phi_inv = (1.0 - 16.0 * Rib) ** 0.75
    return phi_inv


def compute_turbulent_heat(T2m_pt, Ts_C, wind_speed, pressure,
                           RH, z,
                           z0m=0.001, z0t=0.0001, z0h=0.0001,
                           zm=2.0):
    """
    Явный (H) и латентный (LE) турбулентный теплообмен.

    H = cp × ρ₀ × (p/p₀) × k² × U × ΔT × φ⁻¹
        / (ln(zm/z0m) × ln(zm/z0t))

    LE = 0.623 × Lv × ρ₀ × (1/p₀) × k² × U × Δe × φ⁻¹
         / (ln(zm/z0m) × ln(zm/z0h))
    """
    cp = 1005.0      # Дж/(кг·K)
    rho0 = 1.225     # кг/м³
    p0 = 1013.25     # гПа
    k = 0.4          # постоянная Кармана
    Lv = 2.83e6      # скрытая теплота сублимации (Дж/кг)
    e_s = 6.11       # давление пара при 0°C (гПа)

    if wind_speed <= 0.3:
        return 0.0, 0.0

    T2m_K = T2m_pt + 273.15
    delta_T = T2m_pt - Ts_C

    # Число Ричардсона
    if wind_speed > 0:
        Rib = (9.81 * delta_T * (zm - z0m)) / (T2m_K * wind_speed ** 2)
    else:
        Rib = 0.0

    if Rib >= 0.2:
        return 0.0, 0.0

    phi_inv = compute_dimensionless_functions(Rib)

    ln_m = math.log(zm / z0m)
    ln_t = math.log(zm / z0t)
    ln_h = math.log(zm / z0h)

    # Явный теплообмен H
    H = (cp * rho0 * (pressure / p0) * (k ** 2) * wind_speed
         * delta_T * phi_inv / (ln_m * ln_t))

    # Давление пара в воздухе
    e_air = compute_vapor_pressure(T2m_pt, RH, pressure)
    delta_e = e_air - e_s

    # Латентный теплообмен LE
    LE = (0.623 * Lv * rho0 * (1.0 / p0) * (k ** 2)
          * wind_speed * delta_e * phi_inv / (ln_m * ln_h))

    return H, LE


def compute_rain_heat(T2m_pt, Ts_C, precipitation_rate):
    """
    Теплота дождя: Qr = ρw × cw × r × (T2m - Ts)
    """
    if T2m_pt < 2.0 or precipitation_rate <= 0:
        return 0.0

    rho_water = 1000.0
    cp_water = 4186.0
    precip_ms = precipitation_rate / 3600.0 / 1000.0
    return rho_water * cp_water * precip_ms * (T2m_pt - Ts_C)


def compute_ground_heat(ST, T_surface_C,
                        k_r_snow=0.2, k_r_ice=2.2,
                        z_g=0.1, z_0=0.01):
    """
    Теплопоток в грунт: Qg = -kr × (Tg - Ts) / (zg - z0)
    """
    if ST == 1:
        k_r = k_r_snow
        T_g_K = 271.15
    else:
        k_r = k_r_ice
        T_g_K = 272.15

    T_s_K = T_surface_C + 273.15
    Qg = -k_r * (T_g_K - T_s_K) / (z_g - z_0)
    return Qg


def compute_melting_heat(Sin, Sout, Lin, Lout, H, LE, Qr, Qg):
    """
    Энергия таяния: Qm = Sin - Sout + Lin - Lout + H + LE + Qr + Qg
    Только если > 0.
    """
    Qm = (Sin - Sout) + (Lin - Lout) + H + LE + Qr + Qg
    return max(0.0, Qm)


def compute_ablation(Qm, ST, time_step_seconds,
                     rho_snow, rho_ice, L_fs, L_fi):
    """
    Абляция: A = Qm × dt / Lf × 1000  (в мм в.э.)
    """
    if Qm <= 0:
        return 0.0

    if ST == 1:
        L_f = L_fs
    else:
        L_f = L_fi

    melting_energy = Qm * time_step_seconds
    melted_mass = melting_energy / L_f
    water_volume = melted_mass / 1000.0
    ablation_mm = water_volume * 1000.0

    return ablation_mm


# ==================== СОЗДАНИЕ ТОЧЕК ====================
def create_research_points(dem_tif, glacier_shp, num_points=100):
    """
    Создаёт точки на леднике.
    ВАЖНО: точки 94 и AWS2(96) создаются с заданными координатами!
    """
    print(f"Создаём точки (цель: {num_points})...")

    # ЭТАЛОННЫЕ КООРДИНАТЫ
    POINT_94_X = 525285
    POINT_94_Y = 6300765
    POINT_94_Z = 2563

    AWS2_X = 525465  # ИСПРАВЛЕНО!
    AWS2_Y = 6300765  # ИСПРАВЛЕНО!
    AWS2_Z = 2549  # ИСПРАВЛЕНО!

    with rasterio.open(dem_tif) as src:
        glacier_gdf = gpd.read_file(glacier_shp)
        if glacier_gdf.crs != src.crs:
            glacier_gdf = glacier_gdf.to_crs(src.crs)

        points = []

        # =====================================================
        # Добавляем точку 94
        # =====================================================
        print(f"Добавляем точку 94: X={POINT_94_X}, Y={POINT_94_Y}")

        try:
            row_94, col_94 = src.index(POINT_94_X, POINT_94_Y)
            window = rasterio.windows.Window(col_94, row_94, 1, 1)
            z_94 = src.read(1, window=window)[0, 0]

            print(f"  DEM: row={row_94}, col={col_94}, Z={z_94:.1f} (ожидалось {POINT_94_Z})")

            point_94_geom = gpd.points_from_xy([POINT_94_X], [POINT_94_Y])[0]
            points.append({
                'cat': 94,
                'x': POINT_94_X,
                'y': POINT_94_Y,
                'z': z_94,
                'row': row_94,
                'col': col_94,
                'geometry': point_94_geom
            })
        except Exception as e:
            print(f"  ✗ Ошибка добавления точки 94: {e}")

        # =====================================================
        # Добавляем точку AWS2 (cat=96)
        # =====================================================
        print(f"Добавляем точку AWS2 (96): X={AWS2_X}, Y={AWS2_Y}")

        try:
            row_aws2, col_aws2 = src.index(AWS2_X, AWS2_Y)
            window = rasterio.windows.Window(col_aws2, row_aws2, 1, 1)
            z_aws2 = src.read(1, window=window)[0, 0]

            print(f"  DEM: row={row_aws2}, col={col_aws2}, Z={z_aws2:.1f} (ожидалось {AWS2_Z})")

            point_aws2_geom = gpd.points_from_xy([AWS2_X], [AWS2_Y])[0]
            points.append({
                'cat': 96,
                'x': AWS2_X,
                'y': AWS2_Y,
                'z': z_aws2,
                'row': row_aws2,
                'col': col_aws2,
                'geometry': point_aws2_geom
            })
        except Exception as e:
            print(f"  ✗ Ошибка добавления точки AWS2: {e}")

        # =====================================================
        # Добавляем остальные точки
        # =====================================================
        cat_counter = 1

        for j in range(0, src.height):
            for i in range(0, src.width):
                while cat_counter in [94, 96]:
                    cat_counter += 1

                if len(points) >= num_points:
                    break

                x, y = src.xy(j, i)
                point_geom = gpd.points_from_xy([x], [y])[0]

                if glacier_gdf.contains(point_geom).any():
                    window = rasterio.windows.Window(i, j, 1, 1)
                    z = src.read(1, window=window)[0, 0]

                    if not np.isnan(z) and z > -9999:
                        points.append({
                            'cat': cat_counter,
                            'x': x, 'y': y, 'z': z,
                            'row': j, 'col': i,
                            'geometry': point_geom
                        })
                        cat_counter += 1

            if len(points) >= num_points:
                break

        points_gdf = gpd.GeoDataFrame(points, crs=src.crs)

        # Проверка
        p94 = points_gdf[points_gdf['cat'] == 94]
        p96 = points_gdf[points_gdf['cat'] == 96]

        if not p94.empty:
            print(f"\n✓ Точка 94: Z={p94.iloc[0]['z']:.1f}")
        if not p96.empty:
            print(f"✓ Точка AWS2 (96): Z={p96.iloc[0]['z']:.1f}")

        print(f"✓ Всего создано точек: {len(points_gdf)}")
        return points_gdf

# ==================== ЗАГРУЗКА МЕТЕОДАННЫХ ====================
def load_real_aws_data(excel_file="Test_model.xlsx",
                       sheet_name="AWS2_30min"):
    """Загружает реальные метеоданные из Excel"""
    try:
        print(f"Загружаем метеоданные из {excel_file}...")
        df = pd.read_excel(excel_file, sheet_name=sheet_name,
                           header=2)

        column_mapping = {
            'Sin': 'Sin_AWS2',
            'Sout': 'Sout_AWS2',
            'Lin': 'Lin_AWS2',
            'T2m': 'T2m_AWS2',
            'RH2m': 'RH_AWS2',
            'W2m': 'wind_speed',
            'p': 'pressure',
            'Prec': 'precipitation',
            'α': 'alpha_AWS2'
        }
        df = df.rename(columns=column_mapping)

        if 'Дата&Время' in df.columns:
            df['datetime'] = pd.to_datetime(df['Дата&Время'])

        df = df.dropna(subset=['datetime'])
        df = df.sort_values('datetime').reset_index(drop=True)

        print(f"✓ Загружено {len(df)} записей")
        print(f"  Диапазон: {df['datetime'].min()} — "
              f"{df['datetime'].max()}")
        return df

    except Exception as e:
        print(f"✗ Ошибка загрузки метеоданных: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_aws_data_at_time(aws_df, target_datetime):
    """Возвращает метеоданные для конкретного времени"""

    def safe_float(value, default=0.0):
        try:
            if pd.isna(value) or value is None:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    # Точное совпадение
    mask = aws_df['datetime'] == target_datetime
    if mask.any():
        row = aws_df[mask].iloc[0]

        aws_data = {
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
        return aws_data

    # Интерполяция
    before = aws_df[aws_df['datetime'] <= target_datetime]
    after = aws_df[aws_df['datetime'] >= target_datetime]

    if len(before) > 0 and len(after) > 0:
        rb = before.iloc[-1]
        ra = after.iloc[0]

        tb = rb['datetime']
        ta = ra['datetime']
        if ta != tb:
            w = ((target_datetime - tb).total_seconds()
                 / (ta - tb).total_seconds())
        else:
            w = 0.5

        def interp(col, default=0.0):
            v1 = safe_float(rb.get(col), default)
            v2 = safe_float(ra.get(col), default)
            return v1 + (v2 - v1) * w

        aws_data = {
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
        return aws_data

    return {
        'Sin_AWS2': 0.0, 'Sout_AWS2': 0.0,
        'Lin_AWS2': 300.0, 'T2m_AWS2': 5.0,
        'RH_AWS2': 70.0, 'wind_speed': 2.0,
        'pressure': 750.0, 'precipitation': 0.0,
        'alpha_AWS2': 0.5
    }


# ================================================================
#  ГЛАВНАЯ ФУНКЦИЯ — ИСПРАВЛЕННАЯ
# ================================================================
def run_glacier_model(config=CONFIG):
    print("=" * 60)
    print("ЗАПУСК МОДЕЛИ ТАЯНИЯ ЛЕДНИКА")
    print("=" * 60)

    ensure_dir(config["output_dir"])

    # 1. Загрузка метеоданных
    aws_df = load_real_aws_data()
    if aws_df.empty:
        print("✗ Нет метеоданных!")
        return

    # 2. Создание точек
    points_gdf = create_research_points(
        config["dem_tif"], config["glacier_shp"]
    )
    if points_gdf.empty:
        raise Exception("Не удалось создать точки!")

    # 3. Временной диапазон
    start = pd.to_datetime(config["period_start"])
    end = pd.to_datetime(config["period_end"])
    step_min = config["time_step_minutes"]
    step_sec = step_min * 60
    all_times = pd.date_range(start, end, freq=f'{step_min}min')

    print(f"\nРасчёт: {len(points_gdf)} точек × "
          f"{len(all_times)} временных шагов "
          f"= {len(points_gdf) * len(all_times)} строк")

    results = []

    # 4. GRASS сессия
    with Session(gisdb=GRASS_DB, location=LOCATION,
                 mapset=MAPSET) as sess:

        print("✓ GRASS session started")

        # Подготовка GRASS
        gs.run_command('g.region', raster='DEM', quiet=True)

        # Вычисляем slope/aspect если нужно
        if not gs.find_file('slope', element='cell')['file']:
            print("Вычисляем slope и aspect...")
            gs.run_command(
                'r.slope.aspect',
                elevation='DEM',
                slope='slope',
                aspect='aspect',
                overwrite=True
            )

        # Импорт точек
        tmp_shp = os.path.join(tempfile.gettempdir(),
                               "research_points.shp")
        points_gdf.to_file(tmp_shp)
        gs.run_command(
            'v.in.ogr', input=tmp_shp, output='points',
            overwrite=True, flags='o', quiet=True
        )
        gs.run_command(
            'v.db.addcolumn', map='points',
            columns='G double precision',
            quiet=True
        )
        print("✓ Данные подготовлены в GRASS")

        print("Создаём файлы горизонта для учёта затенения...")
        gs.run_command(
            'r.horizon',
            elevation='DEM',
            direction=0,
            step=5,
            output='horizon',
            overwrite=True,
            quiet=True
        )

        # Находим cat точки AWS2
        aws2_cat = 96
        aws2_point = points_gdf[points_gdf['cat'] == aws2_cat]
        if aws2_point.empty:
            # Берём точку ближайшую к z_aws2
            diffs = abs(points_gdf['z'] - config['z_aws2'])
            aws2_idx = diffs.idxmin()
            aws2_cat = points_gdf.loc[aws2_idx, 'cat']
            print(f"  AWS2 точка: cat={aws2_cat}, "
                  f"z={points_gdf.loc[aws2_idx, 'z']:.1f}")

        # ========================================
        #  ГЛАВНЫЙ ЦИКЛ ПО ВРЕМЕНИ
        # ========================================
        prev_day = -1
        total_steps = len(all_times)

        for step_i, current_time in enumerate(all_times):
            day_of_year = current_time.timetuple().tm_yday

            # Прогресс
            if current_time.day != prev_day:
                prev_day = current_time.day
                print(f"\n--- {current_time.strftime('%Y-%m-%d')} "
                      f"(шаг {step_i+1}/{total_steps}) ---")

            # Солнечное время
            solar_time = civil_to_solar_time(
                current_time,
                config["longitude"],
                config["timezone"]
            )

            # Метеоданные
            aws_data = get_aws_data_at_time(aws_df, current_time)

            # ============================================
            #  ИСПРАВЛЕНИЕ: ПРОВЕРЯЕМ СОЛНЕЧНОЕ ВРЕМЯ
            #  r.sun принимает time от 0 до 24
            #  Если ночь — радиация = 0, не нужно
            #  вызывать r.sun
            # ============================================
            G_values = {}
            rasters_to_cleanup = []

            if 0 < solar_time < 24:
                # Запускаем r.sun с учётом затенения
                glob_map, temp_rasters = run_rsun_instantaneous(
                    day_of_year, solar_time,
                    f"{day_of_year}_{current_time.strftime('%H%M')}"
                )

                if glob_map:
                    G_values = extract_raster_at_points(
                        glob_map,
                        points_gdf['cat'].tolist()
                    )

                    # Запоминаем растры для очистки
                    rasters_to_cleanup = temp_rasters if temp_rasters else []

            # Значение G в точке AWS2
            G_AWS2 = G_values.get(aws2_cat, 0.0)

            # Для каждой точки
            for idx, point in points_gdf.iterrows():
                cat = point['cat']
                z = point['z']
                G_cell = G_values.get(cat, 0.0)

                # ------- Sin_cell (ИСПРАВЛЕНО) -------
                Sin_cell = compute_Sin_cell_corrected(
                    aws_data['Sin_AWS2'], G_cell, G_AWS2
                )

                # ------- Температура на высоте -------
                T2m_pt = compute_T2m_at_z(
                    aws_data['T2m_AWS2'],
                    config["kt"], z, config["z_aws2"]
                )

                # ------- Тип поверхности -------
                ST = 1 if z > config["bsl"] else 0

                # ------- Альбедо -------
                Ta = 50  # дней с последнего снегопада
                alpha = compute_albedo(
                    ST, T2m_pt, Ta,
                    config["kSS"], config["kT2m"],
                    config["kTa"], config["c_alpha"]
                )

                # ------- Sout -------
                Sout = compute_Sout(alpha, Sin_cell)

                # ------- Lin (из метеостанции) -------
                Lin = aws_data['Lin_AWS2']

                # ------- Итерация 1: Lout с Qm=0 -------
                Lout_1, Ts_1 = compute_Lout(
                    config["epsilon"], config["sigma"], ST, 0
                )

                H_1, LE_1 = compute_turbulent_heat(
                    T2m_pt, Ts_1, aws_data['wind_speed'],
                    aws_data['pressure'], aws_data['RH_AWS2'], z
                )

                Qr_1 = compute_rain_heat(
                    T2m_pt, Ts_1, aws_data['precipitation']
                )

                Qg_1 = compute_ground_heat(ST, Ts_1)

                Qm_1 = compute_melting_heat(
                    Sin_cell, Sout, Lin, Lout_1,
                    H_1, LE_1, Qr_1, Qg_1
                )

                # ------- Итерация 2: уточняем Lout -------
                Lout, Ts = compute_Lout(
                    config["epsilon"], config["sigma"], ST, Qm_1
                )

                H, LE = compute_turbulent_heat(
                    T2m_pt, Ts, aws_data['wind_speed'],
                    aws_data['pressure'], aws_data['RH_AWS2'], z
                )

                Qr = compute_rain_heat(
                    T2m_pt, Ts, aws_data['precipitation']
                )

                Qg = compute_ground_heat(ST, Ts)

                Rnet, Snet, Lnet = compute_Rnet(
                    Sin_cell, Sout, Lin, Lout
                )

                Qm = compute_melting_heat(
                    Sin_cell, Sout, Lin, Lout,
                    H, LE, Qr, Qg
                )

                ablation = compute_ablation(
                    Qm, ST, step_sec,
                    config["rho_snow"], config["rho_ice"],
                    config["L_fs"], config["L_fi"]
                )

                # Сохраняем
                results.append({
                    'datetime': current_time,
                    'day_of_year': day_of_year,
                    'solar_time': round(solar_time, 2),
                    'cat': cat,
                    'z': z,
                    'ST': ST,
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
                    'wind_speed': round(
                        aws_data['wind_speed'], 2
                    ),
                    'RH': round(aws_data['RH_AWS2'], 2),
                    'pressure': round(aws_data['pressure'], 2),
                    'H': round(H, 2),
                    'LE': round(LE, 2),
                    'Qr': round(Qr, 2),
                    'Qg': round(Qg, 2),
                    'Qm': round(Qm, 2),
                    'ablation_mm': round(ablation, 4),
                })

            # Очистка временных растров (экономия памяти!)
            if rasters_to_cleanup:
                cleanup_temp_rasters(rasters_to_cleanup)

    # ========================================
    #  СОХРАНЕНИЕ РЕЗУЛЬТАТОВ
    # ========================================
    print("\n" + "=" * 60)
    print("СОХРАНЕНИЕ РЕЗУЛЬТАТОВ")
    print("=" * 60)

    results_df = pd.DataFrame(results)

    if results_df.empty:
        print("⚠ Результаты пусты!")
        return

    # Основной CSV
    out_csv = Path(config["output_dir"]) / "model_results.csv"
    results_df.to_csv(out_csv, index=False)
    print(f"✓ CSV: {out_csv} ({len(results_df)} строк)")

    # Excel с несколькими листами
    out_xlsx = Path(config["output_dir"]) / "model_results.xlsx"
    with pd.ExcelWriter(out_xlsx, engine='openpyxl') as writer:
        # Лист model_30min — все данные
        results_df.to_excel(writer, sheet_name='model_30min',
                            index=False)

        # Сводка по дням и точкам
        daily = results_df.groupby(
            [results_df['datetime'].dt.date, 'cat']
        ).agg({
            'Sin_cell': 'sum',
            'Qm': 'sum',
            'ablation_mm': 'sum',
            'z': 'first',
            'T2m': 'mean'
        }).reset_index()
        daily.columns = ['date', 'cat', 'Sin_total',
                          'Qm_total', 'ablation_total',
                          'z', 'T2m_mean']
        daily.to_excel(writer, sheet_name='daily_summary',
                        index=False)

        # Сводка по точкам
        point_summary = results_df.groupby('cat').agg({
            'ablation_mm': 'sum',
            'Qm': 'mean',
            'z': 'first',
            'T2m': 'mean'
        }).reset_index()
        point_summary.to_excel(writer,
                                sheet_name='point_summary',
                                index=False)

    print(f"✓ Excel: {out_xlsx}")

    # Статистика
    print(f"\n--- СТАТИСТИКА ---")
    print(f"Точек: {results_df['cat'].nunique()}")
    print(f"Временных шагов: "
          f"{results_df['datetime'].nunique()}")
    print(f"Всего строк: {len(results_df)}")
    print(f"Sin_cell: min={results_df['Sin_cell'].min():.1f}, "
          f"max={results_df['Sin_cell'].max():.1f}, "
          f"mean={results_df['Sin_cell'].mean():.1f}")
    print(f"Qm: min={results_df['Qm'].min():.1f}, "
          f"max={results_df['Qm'].max():.1f}, "
          f"mean={results_df['Qm'].mean():.1f}")
    print(f"Абляция суммарная: "
          f"{results_df['ablation_mm'].sum():.2f} мм")

    print("\n✓ ГОТОВО!")


# ==================== ЗАПУСК ====================
if __name__ == "__main__":
    run_glacier_model()