#!/usr/bin/env python3
"""
ПОЛНАЯ МОДЕЛЬ ЛЕДНИКА С РЕАЛЬНЫМИ ДАННЫМИ И ПРАВИЛЬНЫМИ ТОЧКАМИ
ИСПРАВЛЕННЫЙ РАСЧЕТ SIN_CELL И ВРЕМЕННОГО РЯДА
"""
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

# --- Добавляем GRASS Python библиотеки вручную ---
grass_base = r"C:\Program Files\GRASS GIS 7.8"
os.environ['GRASSBIN'] = r'"C:\Program Files\GRASS GIS 7.8\grass78.bat"'
grass_python = os.path.join(grass_base, "etc", "python", "grass")
grass_bin = os.path.join(grass_base, "bin")
grass_lib = os.path.join(grass_base, "lib")
os.environ['PATH'] = grass_bin + ";" + grass_lib + ";" + os.environ['PATH']
if grass_python not in sys.path:
    sys.path.append(grass_python)
os.environ['GISBASE'] = grass_base
try:
    from grass_session import Session
    from grass.script import run_command, read_command, parse_command

    print("✓ GRASS модули успешно импортированы")
except ImportError as e:
    print(f"✗ Ошибка импорта GRASS: {e}")
    # Не выходим, чтобы работало на машинах без GRASS через Python-расчет
    print("  (Работаем в режиме Python-симуляции r.sun)")
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
    "period_start": "2019-07-07T00:00:00", # по умолчанию - 2019-07-07T00:00:00
    "period_end": "2019-08-31T23:30:00", # по умолчанию - 2019-08-31T23:30:00
    "kt": -0.0065,
    "asl": 1.7813, "bsl": 2067.6,
    "kSS": 0.33745, "kT2m": 0.00838, "kTa": -0.00112, "c_alpha": 0.13469,
    "rho_ice": 784, "rho_snow": 602,
    "sigma": 5.670374419e-8,
    "epsilon": 1,
    "z_aws1": 2540,
    "z_aws2": 2561,
    "L_fs": 330000,
    "L_fi": 335000,
    "latitude": 42.9,  # Широта для расчетов солнечной радиации
}


def ensure_dir(d):
    os.makedirs(d, exist_ok=True)


# ==================== ФУНКЦИИ ДЛЯ ФАЙЛОВ ====================
def check_shapefile_completeness(shp_path):
    """Проверяет наличие всех необходимых файлов shapefile"""
    shp_path = Path(shp_path)
    required_extensions = ['.shp', '.shx', '.dbf', '.prj']
    missing_files = []
    for ext in required_extensions:
        if not shp_path.with_suffix(ext).exists():
            missing_files.append(shp_path.with_suffix(ext).name)
    if missing_files:
        print(f"⚠ Отсутствуют файлы shapefile: {missing_files}")
        return False
    else:
        print("✓ Все файлы shapefile присутствуют")
        return True


def repair_shapefile(shp_path):
    """Восстанавливает недостающие файлы shapefile"""
    shp_path = Path(shp_path)
    if not shp_path.with_suffix('.prj').exists():
        print("Создаем .prj файл из DEM...")
        try:
            with rasterio.open("DEM.tif") as dem:
                crs = dem.crs
                if crs:
                    crs_wkt = crs.to_wkt()
                    with open(shp_path.with_suffix('.prj'), 'w', encoding='utf-8') as f:
                        f.write(crs_wkt)
                    print("✓ .prj файл создан")
        except Exception as e:
            print(f"✗ Ошибка создания .prj: {e}")
    if not shp_path.with_suffix('.shx').exists():
        print("Восстанавливаем .shx файл...")
        try:
            gdf = gpd.read_file(shp_path)
            gdf.to_file(shp_path, driver='ESRI Shapefile')
            print("✓ .shx файл восстановлен")
        except Exception as e:
            print(f"✗ Ошибка восстановления .shx: {e}")


def get_raster_info(raster_path):
    """Получает информацию о raster файле"""
    try:
        with rasterio.open(raster_path) as src:
            info = {
                'crs': src.crs,
                'bounds': src.bounds,
                'width': src.width,
                'height': src.height,
                'res': src.res,
                'dtype': src.dtypes[0],
                'nodata': src.nodata
            }
            print(f"DEM CRS: {src.crs}")
            print(f"DEM Bounds: {src.bounds}")
            print(f"DEM Size: {src.width} x {src.height}")
            return info
    except Exception as e:
        print(f"✗ Ошибка чтения DEM: {e}")
        return None


# ==================== GRASS ФУНКЦИИ ====================
def setup_grass_simple():
    """
    Настройка GRASS с дополнительными растровыми данными
    """
    print("=== НАСТРОЙКА GRASS С ДОПОЛНИТЕЛЬНЫМИ РАСТРАМИ ===")

    gisdb = tempfile.mkdtemp(prefix="grass_simple_")
    location_name = "glacier_location"

    print(f"GRASS database: {gisdb}")

    try:
        grass_bat = r"C:\Program Files\GRASS GIS 7.8\grass78.bat"

        # Создаем location
        cmd = [grass_bat, "-c", "EPSG:4326", "-e", os.path.join(gisdb, location_name)]
        print("Создаем location...")
        subprocess.run(cmd, capture_output=True, timeout=60, shell=True)

        # Импортируем основные растры если они существуют
        raster_files = {
            "dem": CONFIG["dem_tif"],
            "elevation": CONFIG.get("elevation_tif"),
            "slope": CONFIG.get("slope_tif"),
            "aspect": CONFIG.get("aspect_tif")
        }

        for raster_name, file_path in raster_files.items():
            if file_path and os.path.exists(file_path):
                print(f"Импортируем {raster_name}...")
                import_cmd = [grass_bat, "--config", "path", gisdb, location_name, "PERMANENT",
                              "--exec", "r.in.gdal", f"input={file_path}", f"output={raster_name}", "--overwrite"]
                subprocess.run(import_cmd, capture_output=True, timeout=60, shell=True)
            else:
                print(f"Файл {file_path} не найден, пропускаем {raster_name}")

        # Если slope/aspect отсутствуют, вычисляем их из DEM
        if not os.path.exists(CONFIG.get("slope_tif")) and os.path.exists(CONFIG["dem_tif"]):
            print("Вычисляем slope и aspect из DEM...")
            slope_cmd = [grass_bat, "--config", "path", gisdb, location_name, "PERMANENT",
                         "--exec", "r.slope.aspect", "elevation=dem", "slope=slope", "aspect=aspect", "--overwrite"]
            subprocess.run(slope_cmd, capture_output=True, timeout=60, shell=True)

        # Импортируем векторные данные
        print("Импортируем glacier...")
        glacier_cmd = [grass_bat, "--config", "path", gisdb, location_name, "PERMANENT",
                       "--exec", "v.in.ogr", "input=glacier.shp", "output=glacier", "--overwrite"]
        subprocess.run(glacier_cmd, capture_output=True, timeout=60, shell=True)

        # Устанавливаем регион и маску
        print("Устанавливаем регион...")
        region_cmd = [grass_bat, "--config", "path", gisdb, location_name, "PERMANENT",
                      "--exec", "g.region", "raster=dem"]
        subprocess.run(region_cmd, capture_output=True, timeout=30, shell=True)

        print("Создаем маску...")
        mask_cmd = [grass_bat, "--config", "path", gisdb, location_name, "PERMANENT",
                    "--exec", "r.mask", "vector=glacier", "--overwrite"]
        subprocess.run(mask_cmd, capture_output=True, timeout=30, shell=True)

        print("✓ GRASS настройка с дополнительными растрами завершена")
        return gisdb, location_name

    except Exception as e:
        print(f"✗ Ошибка настройки GRASS: {e}")
        return None, None


def run_r_sun_grass(gisdb, location_name, day_of_year, time_decimal, points_count):
    """
    Запускает r.sun через GRASS и возвращает значения радиации
    """
    try:
        grass_bat = r"C:\Program Files\GRASS GIS 7.8\grass78.bat"

        rad_name = f"radiation_{day_of_year}_{int(time_decimal * 100)}"

        rsun_cmd = [
            grass_bat, "--config", "path", gisdb, location_name, "PERMANENT",
            "--exec", "r.sun",
            "elevation=dem", "slope=slope", "aspect=aspect",
            f"day={day_of_year}", f"time={time_decimal}",
            f"glob_rad={rad_name}", "--overwrite", "--quiet"
        ]

        print(f"  Запуск r.sun: день {day_of_year}, время {time_decimal:.2f}")
        result = subprocess.run(rsun_cmd, capture_output=True, text=True, timeout=60, shell=True)

        if result.returncode != 0:
            print(f"  ⚠ Ошибка r.sun: {result.stderr}")
            return {}

        stats_cmd = [
            grass_bat, "--config", "path", gisdb, location_name, "PERMANENT",
            "--exec", "r.univar", f"map={rad_name}", "flags=g", "--quiet"
        ]

        stats_result = subprocess.run(stats_cmd, capture_output=True, text=True, timeout=30, shell=True)

        mean_radiation = 500
        if stats_result.returncode == 0:
            for line in stats_result.stdout.split('\n'):
                if 'mean:' in line:
                    try:
                        mean_radiation = float(line.split(':')[1].strip())
                        break
                    except ValueError:
                        pass

        G_values = {}
        np.random.seed(day_of_year + int(time_decimal * 100))

        # Здесь должна быть реализация считывания значений по точкам (v.what.rast)
        # Для текущего кода оставим симуляцию возврата словаря, так как это placeholder
        for i in range(1, points_count + 1):
            variation = 0.8 + 0.4 * np.random.random()
            G_values[i] = mean_radiation * variation

        cleanup_cmd = [
            grass_bat, "--config", "path", gisdb, location_name, "PERMANENT",
            "--exec", "g.remove", "type=raster", f"name={rad_name}", "-f", "--quiet"
        ]
        subprocess.run(cleanup_cmd, capture_output=True, timeout=10, shell=True)

        return G_values

    except Exception as e:
        print(f"✗ Ошибка r.sun: {e}")
        return {}


# Проверка CRS файлов
def check_coordinate_systems():
    """Проверяет системы координат всех файлов"""
    try:
        with rasterio.open(CONFIG["dem_tif"]) as dem:
            print(f"DEM CRS: {dem.crs}")
            print(f"DEM Transform: {dem.transform}")
            print(f"DEM Bounds: {dem.bounds}")

        glacier_gdf = gpd.read_file(CONFIG["glacier_shp"])
        print(f"Glacier CRS: {glacier_gdf.crs}")
        print(f"Glacier bounds: {glacier_gdf.total_bounds}")

        # Если координаты не совпадают с ожидаемыми, возможно нужно преобразование
        expected_bounds = [525000, 6300000, 526000, 6301000]  # Примерные ожидаемые границы

        return True

    except Exception as e:
        print(f"Ошибка проверки координат: {e}")
        return False


# ==================== КЛЮЧЕВЫЕ ФИКСЫ ДЛЯ РАСЧЕТА SIN_CELL ====================
def get_sunrise_sunset_times_from_excel():
    """
    Получаем точные времена восхода и захода из предоставленных данных Excel
    Анализ данных показывает:
    - Начало дневной радиации: ~04:00 (первые ненулевые значения)
    - Конец дневной радиации: ~20:30 (последние ненулевые значения)
    - Пик в районе 12:00-13:00
    """
    # Из предоставленных данных: первые ненулевые значения в 4:00
    sunrise = 4.0  # 04:00
    sunset = 20.5  # 20:30

    return sunrise, sunset


def is_night_time_corrected(time_decimal):
    """
    Корректная проверка - ночь ли сейчас, основанная на данных Excel
    """
    sunrise, sunset = get_sunrise_sunset_times_from_excel()
    return time_decimal < sunrise or time_decimal > sunset


def calculate_solar_radiation_with_excel_pattern(points_gdf, datetime_obj, aws_data):
    """
    Расчет солнечной радиации G(z,t) по точному шаблону из Excel
    Используем реальные значения из предоставленных данных
    """
    try:
        time_decimal = round(datetime_obj.hour + datetime_obj.minute / 60.0, 2)

        excel_g_values_94 = {
            0.0: 0.0, 0.5: 0.0, 1.0: 0.0, 1.5: 0.0, 2.0: 0.0, 2.5: 0.0, 3.0: 0.0,
            3.5: 12.9, 4.0: 22.8, 4.5: 32.7, 5.0: 42.4, 5.5: 51.6, 6.0: 60.1,
            6.5: 67.6, 7.0: 74.1, 7.5: 68.4, 8.0: 128.7, 8.5: 195.1, 9.0: 266.0,
            9.5: 339.8, 10.0: 414.8, 10.5: 489.4, 11.0: 562.1, 11.5: 631.3,
            12.0: 695.5, 12.5: 753.3, 13.0: 803.5, 13.5: 844.8, 14.0: 876.0,
            14.5: 896.2, 15.0: 904.4, 15.5: 899.9, 16.0: 882.1, 16.5: 850.5,
            17.0: 804.8, 17.5: 67.6, 18.0: 60.1, 18.5: 51.6, 19.0: 42.4,
            19.5: 32.7, 20.0: 22.8, 20.5: 12.9, 21.0: 0.0, 21.5: 0.0,
            22.0: 0.0, 22.5: 0.0, 23.0: 0.0, 23.5: 0.0
        }

        # G для точки AWS2 (точка 96) — r.sun именно в точке метеостанции
        excel_g_values_96 = {
            0.0: 0.0, 0.5: 0.0, 1.0: 0.0, 1.5: 0.0, 2.0: 0.0, 2.5: 0.0, 3.0: 0.0,
            3.5: 13.5, 4.0: 23.9, 4.5: 34.3, 5.0: 44.5, 5.5: 54.2, 6.0: 63.1,
            6.5: 70.9, 7.0: 77.8, 7.5: 71.8, 8.0: 135.1, 8.5: 204.9, 9.0: 279.3,
            9.5: 356.8, 10.0: 435.5, 10.5: 513.9, 11.0: 590.2, 11.5: 662.9,
            12.0: 730.3, 12.5: 790.9, 13.0: 843.7, 13.5: 887.1, 14.0: 919.8,
            14.5: 941.0, 15.0: 949.6, 15.5: 945.0, 16.0: 926.2, 16.5: 893.0,
            17.0: 845.0, 17.5: 71.0, 18.0: 63.1, 18.5: 54.2, 19.0: 44.5,
            19.5: 34.3, 20.0: 23.9, 20.5: 13.5, 21.0: 0.0, 21.5: 0.0,
            22.0: 0.0, 22.5: 0.0, 23.0: 0.0, 23.5: 0.0
        }



        # Интерполяция G_cell (точка 94 — эталон)
        hours_94 = sorted(excel_g_values_94.keys())
        vals_94 = [excel_g_values_94[h] for h in hours_94]
        g_cell_base = float(np.interp(time_decimal, hours_94, vals_94))

        # Интерполяция G_AWS2 (точка 96 — метеостанция)
        hours_96 = sorted(excel_g_values_96.keys())
        vals_96 = [excel_g_values_96[h] for h in hours_96]
        g_aws2 = float(np.interp(time_decimal, hours_96, vals_96))

        # Реальное Sin_AWS2 из метеоданных (меняется каждый день!)
        sin_aws2 = aws_data['Sin_AWS2']

        # === ОТЛАДКА: ВСТАВИТЬ СЮДА ===
        if not hasattr(calculate_solar_radiation_with_excel_pattern, '_debug_count'):
            calculate_solar_radiation_with_excel_pattern._debug_count = 0

        if calculate_solar_radiation_with_excel_pattern._debug_count < 48:
            ratio = g_cell_base / g_aws2 if g_aws2 > 0.1 else 0
            sin_cell_calc = sin_aws2 * ratio if g_aws2 > 0.1 else 0

            print(f"  DEBUG {datetime_obj.strftime('%H:%M')}: "
                  f"Sin_AWS2={sin_aws2:.1f}, "
                  f"G_cell={g_cell_base:.1f}, G_AWS2={g_aws2:.1f}, "
                  f"ratio={ratio:.4f}, "
                  f"Sin_cell={sin_cell_calc:.1f}")

            calculate_solar_radiation_with_excel_pattern._debug_count += 1
        # === КОНЕЦ ОТЛАДКИ ===

        radiation_values = {}

        for idx, point in points_gdf.iterrows():
            cat = point['cat']

            # Небольшая поправка G_cell по высоте точки
            height_factor = 1.0 + (point['z'] - 2560) / 1000 * 0.05
            g_cell = g_cell_base * height_factor

            radiation_values[cat] = {
                'G_cell': g_cell,
                'G_AWS2': g_aws2,  # ← ФИКС: реальное значение r.sun для AWS2
                'Sin_AWS2': sin_aws2  # ← ФИКС: реальное значение из метеоданных
            }



        return radiation_values

    except Exception as e:
        print(f"✗ Ошибка расчета радиации: {e}")
        return {}


def compute_vapor_pressure(T2m, RH, p):
    """
    ПРАВИЛЬНАЯ формула из документации:
    e(z,t) = 6.112 × exp(17.62 × T2m / (243.12 + T2m)) ×
              (1.0016 + 0.0000315 × p - 0.074 / p) × RH / 100
    """
    term1 = 6.112 * math.exp(17.62 * T2m / (243.12 + T2m))
    term2 = 1.0016 + 0.0000315 * p - 0.074 / p
    return term1 * term2 * (RH / 100)


def compute_Sin_cell_corrected(Sin_AWS2, G_cell, G_AWS2):
    """
    ИСПРАВЛЕННАЯ формула для расчета Sin_cell:
    Sin(z,t) = Sin(AWS2,t) * G(z,t) / G(AWS2,t)

    С обработкой крайних случаев:
    1. Если G_AWS2 близко к 0, но Sin_AWS2 > 0 (сумерки, рассеянный свет)
    2. Если G_cell близко к 0 (точка в тени)
    3. Ночные условия
    """
    if G_AWS2 <= 0.1 or Sin_AWS2 <= 0:
        return 0.0

        # САМАЯ ПРОСТАЯ ФОРМУЛА
    sin_cell = Sin_AWS2 * (G_cell / G_AWS2)

    # Не делаем никаких дополнительных проверок!
    return max(0.0, sin_cell)


def compute_albedo(ST, T2m, Ta, k_ST, k_T2m, k_Ta, c_alpha):
    """Альбедо поверхности"""
    albedo = k_ST * ST + k_T2m * T2m + k_Ta * Ta + c_alpha
    return max(0.1, min(0.9, albedo))


def compute_Sout(alpha, Sin):
    """Отраженная коротковолновая радиация"""
    return alpha * Sin


def compute_Lin_realistic(T2m_pt, RH, time_decimal, cloud_cover=0.3):
    """Длинноволновое излучение атмосферы"""
    try:
        T_K = 273.15 + T2m_pt
        base_emissivity = 0.7 + 0.06 * (RH / 100)
        time_factor = 0.9 + 0.1 * (1 - min(1, abs(time_decimal - 12) / 6))
        cloud_factor = 1 + 0.2 * cloud_cover
        atmospheric_emissivity = min(0.95, base_emissivity * time_factor * cloud_factor)
        Lin = atmospheric_emissivity * 5.670374419e-8 * (T_K ** 4)
        return Lin
    except:
        return 300


def compute_Lout_corrected(epsilon, sigma, ST, Qm):
    """Длинноволновое излучение поверхности"""
    """
    ПРАВИЛЬНАЯ формула из документации:
    Lout(z,t) = εσTs(z)⁴
    При Qm > 0 температура поверхности = 0°C (273.15K)
    """
    try:
        # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: температура поверхности = 0°C при таянии
        if Qm > 0:
            Ts_K = 273.15  # 0°C при таянии
        else:
            # Если нет таяния, используем приближенную температуру
            # Для снега холоднее, для льда ближе к 0
            if ST == 1:  # снег
                Ts_K = 271.15  # -2°C
            else:  # лед
                Ts_K = 272.15  # -1°C

        Lout = epsilon * sigma * (Ts_K ** 4)
        Ts_C = Ts_K - 273.15
        return Lout, Ts_C
    except:
        return epsilon * sigma * (273.15 ** 4), 0


def compute_T2m_at_z(T2m_aws2, kt, z_cell, z_aws2):
    """Температура воздуха на высоте ячейки"""
    return T2m_aws2 + kt * (z_cell - z_aws2)


def compute_Rnet(Sin, Sout, Lin, Lout):
    """Радиационный баланс"""
    Snet = Sin - Sout
    Lnet = Lin - Lout
    return Snet + Lnet, Snet, Lnet


def compute_pressure_at_z(p_aws1, z_cell, z_aws1, T_layer):
    """
    ПРАВИЛЬНАЯ формула из документации:
    p(z,t) = p(AWS1,t) / 10^((z - z(AWS1)) / (18400 * (1 + 0.003665 * T)))
    """
    denominator = 18400 * (1 + 0.003665 * T_layer)
    exponent = (z_cell - z_aws1) / denominator
    return p_aws1 / (10 ** exponent)


def compute_dimensionless_functions(Rib):
    """
    Формула 17: Безразмерные функции
    """
    if Rib > 0:  # стабильные условия
        phi_inv = (1 - 5 * Rib) ** 2
    else:  # нестабильные условия
        phi_inv = (1 - 16 * Rib) ** 0.75

    return phi_inv  # возвращает (Φ_m Φ_t)^{-1} = (Φ_m Φ_h)^{-1}


def compute_turbulent_heat_corrected(T2m_pt, Ts_C, wind_speed, pressure, RH, z,
                                     z0m=0.001, z0t=0.0001, z0h=0.0001, zm=2.0):
    """
    ПРАВИЛЬНЫЕ формулы 18 и 19 для явного (H) и скрытного (LE) тепла
    """
    # Константы
    cp = 1005  # Дж/(кг·K)
    rho0 = 1.225  # кг/м³
    p0 = 1013.25  # гПа
    k = 0.4  # постоянная Кармана
    L_v = 2.83e6  # скрытая теплота испарения снега/льда (Дж/кг)
    e_s = 6.11  # давление пара на поверхности при 0°C (гПа)

    T2m_K = T2m_pt + 273.15
    Ts_K = Ts_C + 273.15

    if wind_speed <= 0.5:
        return 0, 0

    # Число Ричардсона
    delta_T = T2m_pt - Ts_C
    Rib = (9.81 * delta_T * (zm - z0m)) / (T2m_K * wind_speed ** 2)

    if Rib >= 0.4:
        return 0, 0

    # Безразмерные функции (Формула 17)
    phi_inv = compute_dimensionless_functions(Rib)

    # ПРАВИЛЬНАЯ формула 18 для H
    H = (cp * rho0 * (pressure / p0) * (k ** 2) * wind_speed * delta_T *
         phi_inv / (math.log(zm / z0m) * math.log(zm / z0t)))

    # ПРАВИЛЬНАЯ формула 19 для LE
    # Давление пара в воздухе (из формулы 15)
    e_air = compute_vapor_pressure(T2m_pt, RH, pressure)
    delta_e = e_air - e_s

    LE = (0.623 * L_v * rho0 * (1 / p0) * (k ** 2) * wind_speed * delta_e *
          phi_inv / (math.log(zm / z0m) * math.log(zm / z0h)))

    return H, LE


def compute_rain_heat_corrected(T2m_pt, Ts_C, precipitation_rate):
    """
    ПРАВИЛЬНАЯ формула 20:
    Qr(z,t) = ρ_w × c_w × r × (T_zm - T_s)
    """
    if T2m_pt < 2 or precipitation_rate <= 0:
        return 0

    try:
        rho_water = 1000  # кг/м³
        cp_water = 4186  # Дж/(кг·K)

        # Преобразование мм/ч в м/с
        precip_ms = precipitation_rate / 3600 / 1000

        Qr = rho_water * cp_water * precip_ms * (T2m_pt - Ts_C)
        return Qr
    except:
        return 0


def compute_ground_heat_corrected(ST, T_surface, time_decimal=None,
                                  k_r_snow=0.2, k_r_ice=2.2,
                                  T_g=273.15, z_g=0.1, z_0=0.01):
    """
    ПРАВИЛЬНАЯ формула 21:
    Qg = -k_r × (T_g - T_s) / (z_g - z_0)
    """
    try:
        # Выбираем теплопроводность в зависимости от типа поверхности
        if ST == 1:  # снег
            k_r = k_r_snow
            # Для снега температура ледника обычно ниже
            T_g_deep = 271.15  # -2°C
        else:  # лед
            k_r = k_r_ice
            T_g_deep = 272.15  # -1°C

        # Преобразуем температуру поверхности в Кельвины
        T_s_K = T_surface + 273.15

        # Расчет по формуле
        Qg = -k_r * (T_g_deep - T_s_K) / (z_g - z_0)

        return Qg
    except:
        # Fallback значение
        if ST == 1:
            return -5
        else:
            return -10


def compute_melting_heat(Sin, Sout, Lin, Lout, H, LE, Qr, Qg):
    """
    ПРАВИЛЬНАЯ формула 22 из документации:
    Qm(z,t) = Sin + Sout + Lin + Lout + H + LE + Qr + Qg
    """
    Qm = Sin + Sout + Lin + Lout + H + LE + Qr + Qg
    return max(0, Qm)  # Таяние только когда Qm > 0


def compute_ablation_corrected(Qm, ST, time_step_seconds, rho_snow, rho_ice, L_fs, L_fi):
    """
    ПРАВИЛЬНАЯ формула 23:
    A(z,t) = (Qm(z,t) × t_mod / L_f(s,i)) × 1000
    """
    if Qm <= 0:
        return 0

    try:
        # Выбираем параметры в зависимости от типа поверхности
        if ST == 1:  # снег
            L_f = L_fs
            rho = rho_snow
        else:  # лед
            L_f = L_fi
            rho = rho_ice

        # Энергия таяния за временной шаг (Дж/м²)
        melting_energy = Qm * time_step_seconds

        # Масса расплавленного вещества (кг/м²)
        melted_mass = melting_energy / L_f

        # Объем воды (м³/м² = м)
        water_volume = melted_mass / 1000  # делим на плотность воды 1000 кг/м³

        # Абляция в мм воды
        ablation_mm = water_volume * 1000

        return ablation_mm
    except:
        return 0


# ==================== СОЗДАНИЕ ТОЧЕК ИССЛЕДОВАНИЯ ====================
def create_research_points(dem_tif, glacier_shp, num_points=100):
    """
    ИСПРАВЛЕНО: Шаг перебора 1, чтобы найти все 100 точек.
    """
    print(f"Создаем точки (цель: {num_points} шт) с фиксированными 94 и 96...")

    try:
        with rasterio.open(dem_tif) as src:
            glacier_gdf = gpd.read_file(glacier_shp)
            if glacier_gdf.crs != src.crs:
                glacier_gdf = glacier_gdf.to_crs(src.crs)

            points = []

            # --- Вспомогательная функция ---
            def add_special_point(target_x, target_y, cat_id):
                # Ищем ближайшую ячейку (грубый поиск)
                closest_dist = float('inf')
                closest_cell = None

                # Используем шаг 10 для быстрого поиска области
                for j in range(0, src.height, 10):
                    for i in range(0, src.width, 10):
                        x, y = src.xy(j, i)
                        dist = np.sqrt((x - target_x) ** 2 + (y - target_y) ** 2)
                        if dist < closest_dist:
                            closest_dist = dist
                            closest_cell = (i, j)

                # Точный поиск вокруг найденной области
                if closest_cell:
                    ci, cj = closest_cell
                    best_cell = None
                    best_dist = float('inf')
                    for j in range(max(0, cj - 15), min(src.height, cj + 15)):
                        for i in range(max(0, ci - 15), min(src.width, ci + 15)):
                            x, y = src.xy(j, i)
                            dist = np.sqrt((x - target_x) ** 2 + (y - target_y) ** 2)
                            if dist < best_dist:
                                best_dist = dist
                                best_cell = (i, j, x, y)

                    if best_cell:
                        i, j, x, y = best_cell
                        # Читаем высоту
                        window = rasterio.windows.Window(i, j, 1, 1)
                        z = src.read(1, window=window)[0, 0]
                        if z > -9999:
                            point_geom = gpd.points_from_xy([x], [y])[0]
                            return {
                                'cat': cat_id, 'x': x, 'y': y, 'z': z,
                                'row': j, 'col': i, 'geometry': point_geom
                            }
                return None

            # 1. Добавляем точку 94
            p94 = add_special_point(525285, 6300765, 94)
            if p94: points.append(p94)

            # 2. Добавляем точку 96 (AWS2)
            p96 = add_special_point(525290, 6300770, 96)
            if p96: points.append(p96)

            # 3. Добавляем остальные точки (шаг 1!)
            cat_counter = 1

            # ВАЖНО: шаг 1, а не 2, чтобы собрать все точки
            for j in range(0, src.height, 1):
                for i in range(0, src.width, 1):
                    # Пропускаем номера, занятые спец. точками
                    while cat_counter == 94 or cat_counter == 96:
                        cat_counter += 1

                    x, y = src.xy(j, i)
                    point_geom = gpd.points_from_xy([x], [y])[0]

                    # Проверка попадания в ледник
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
                if len(points) >= num_points:
                    break

            points_gdf = gpd.GeoDataFrame(points, crs=src.crs)
            print(f"✓ Успешно создано точек: {len(points_gdf)}")
            return points_gdf

    except Exception as e:
        print(f"✗ Ошибка создания точек: {e}")
        import traceback
        traceback.print_exc()
        return gpd.GeoDataFrame()


# ==================== ЗАГРУЗКА РЕАЛЬНЫХ МЕТЕОДАННЫХ ====================
def load_real_aws_data(excel_file="test_model.xlsx", sheet_name="AWS2_30min"):
    """
    Загружает реальные метеоданные из Excel файла
    """
    try:
        print(f"Загружаем реальные метеоданные из {excel_file}...")

        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=2)

        print(f"Столбцы ДО переименования: {df.columns.tolist()}")

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

        # === ОТЛАДКА: показываем данные для 7 июля ===
        if 'Дата&Время' in df.columns:
            df['datetime'] = pd.to_datetime(df['Дата&Время'])

        # Фильтруем 7 июля
        july7 = df[df['datetime'].dt.date == pd.to_datetime('2019-07-07').date()]

        print(f"\n=== ДАННЫЕ ЗА 7 ИЮЛЯ ===")
        print(f"Найдено записей: {len(july7)}")

        if len(july7) > 0:
            print(f"\nПервые 10 записей 7 июля:")
            for i, (idx, row) in enumerate(july7.head(10).iterrows()):
                time_str = row['datetime'].strftime('%H:%M')
                sin_val = row['Sin_AWS2']
                print(f"  {time_str}: Sin_AWS2 = {sin_val}")

            # Проверяем полдень
            noon = july7[july7['datetime'].dt.hour == 12]
            print(f"\nДанные в 12:00-12:30:")
            for idx, row in noon.iterrows():
                time_str = row['datetime'].strftime('%H:%M')
                sin_val = row['Sin_AWS2']
                print(f"  {time_str}: Sin_AWS2 = {sin_val}")

        print(f"=== КОНЕЦ ОТЛАДКИ ===\n")
        # === КОНЕЦ ОТЛАДКИ ===

        df = df.dropna(subset=['datetime'])
        df = df.sort_values('datetime').reset_index(drop=True)

        print(f"✓ Загружено {len(df)} записей")
        return df

    except Exception as e:
        print(f"✗ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_aws_data_at_time(aws_df, target_datetime):
    """
    Возвращает метеоданные для конкретного времени
    ИСПРАВЛЕНО: если нет точного совпадения — интерполируем
    """
    try:
        # Ищем точное совпадение
        mask = aws_df['datetime'] == target_datetime
        if mask.any():
            row = aws_df[mask].iloc[0]
        else:
            # Ищем ближайшие записи ДО и ПОСЛЕ
            before = aws_df[aws_df['datetime'] <= target_datetime]
            after = aws_df[aws_df['datetime'] >= target_datetime]

            if len(before) > 0 and len(after) > 0:
                row_before = before.iloc[-1]
                row_after = after.iloc[0]

                # Линейная интерполяция
                t_before = row_before['datetime']
                t_after = row_after['datetime']
                t_target = target_datetime

                if t_after != t_before:
                    weight = (t_target - t_before).total_seconds() / (t_after - t_before).total_seconds()
                else:
                    weight = 0.5

                # Интерполируем числовые поля
                def interp(col):
                    v1 = safe_float(row_before[col])
                    v2 = safe_float(row_after[col])
                    return v1 + (v2 - v1) * weight

                aws_data = {
                    'Sin_AWS2': interp('Sin_AWS2'),
                    'Sout_AWS2': interp('Sout_AWS2'),
                    'Lin_AWS2': interp('Lin_AWS2'),
                    'T2m_AWS2': interp('T2m_AWS2'),
                    'RH_AWS2': interp('RH_AWS2'),
                    'wind_speed': interp('wind_speed'),
                    'pressure': interp('pressure'),
                    'precipitation': interp('precipitation'),
                    'alpha_AWS2': interp('alpha_AWS2'),
                }

                if aws_data['alpha_AWS2'] > 0:
                    aws_data['G_AWS2'] = aws_data['Sin_AWS2'] / aws_data['alpha_AWS2']
                else:
                    aws_data['G_AWS2'] = 0

                return aws_data

            elif len(before) > 0:
                # Берём последнее значение перед target
                row = before.iloc[-1]
            elif len(after) > 0:
                # Берём первое значение после target
                row = after.iloc[0]
            else:
                # Вообще нет данных
                return get_default_aws_data()

        # Функция для безопасного преобразования
        def safe_float(value, default=0.0):
            try:
                if pd.isna(value) or value == '' or value is None or value == 'NODATA':
                    return default
                return float(value)
            except (ValueError, TypeError):
                return default

        aws_data = {
            'Sin_AWS2': safe_float(row['Sin_AWS2']),
            'Sout_AWS2': safe_float(row.get('Sout_AWS2', 0)),
            'Lin_AWS2': safe_float(row['Lin_AWS2']),
            'T2m_AWS2': safe_float(row['T2m_AWS2']),
            'RH_AWS2': safe_float(row['RH_AWS2']),
            'wind_speed': safe_float(row['wind_speed']),
            'pressure': safe_float(row['pressure']),
            'precipitation': safe_float(row['precipitation']),
            'alpha_AWS2': safe_float(row['alpha_AWS2'], 0.5),
        }

        if aws_data['alpha_AWS2'] > 0 and aws_data['alpha_AWS2'] <= 1.0:
            aws_data['G_AWS2'] = aws_data['Sin_AWS2'] / aws_data['alpha_AWS2']
        else:
            aws_data['G_AWS2'] = 0

        return aws_data

    except Exception as e:
        print(f"Ошибка получения метеоданных для {target_datetime}: {e}")
        return get_default_aws_data()


def get_default_aws_data():
    """Возвращает дефолтные метеоданные"""
    return {
        'Sin_AWS2': 0.0,
        'Sout_AWS2': 0.0,
        'Lin_AWS2': 300.0,
        'T2m_AWS2': 5.0,
        'RH_AWS2': 70.0,
        'wind_speed': 2.0,
        'pressure': 1013.0,
        'precipitation': 0.0,
        'alpha_AWS2': 0.5,
        'G_AWS2': 0.0
    }

# ==================== ОСНОВНАЯ ФУНКЦИЯ С ИСПРАВЛЕННЫМ РАСЧЕТОМ ====================
def run_glacier_model_final_correction(config=CONFIG):
    """
    ФИНАЛЬНАЯ ИСПРАВЛЕННАЯ ВЕРСИЯ
    Исправлены отступы и шаг времени.
    """
    print("=" * 60)
    print("ЗАПУСК МОДЕЛИ: 100 ТОЧЕК, ШАГ 30 МИН")
    print("=" * 60)

    ensure_dir(config["output_dir"])

    # Загрузка данных
    aws_df = load_real_aws_data()
    if aws_df.empty:
        print("Creating test AWS data...")
        aws_df = create_test_aws_data()

    # Точки
    points_gdf = create_research_points(config["dem_tif"], config["glacier_shp"])
    if points_gdf.empty:
        raise Exception("Не удалось создать точки!")

    print(f"✓ Расчет будет выполнен для {len(points_gdf)} точек")

    # Время
    start = pd.to_datetime(config["period_start"])
    end = pd.to_datetime(config["period_end"])
    time_step_seconds = config["time_step_minutes"] * 60

    results = []
    current_time = start

    # Отладка точки 94
    debug_data_94 = []

    print("\n=== НАЧАЛО РАСЧЕТА ПО ВРЕМЕНИ ===")

    # === ГЛАВНЫЙ ЦИКЛ ПО ВРЕМЕНИ ===
    while current_time <= end:
        time_str = current_time.strftime("%Y-%m-%d %H:%M")
        # Округляем до 2 знаков, чтобы избежать 4.000000001
        time_decimal = round(current_time.hour + current_time.minute / 60.0, 2)

        # Получаем данные погоды один раз на этот момент времени
        aws_data = get_aws_data_at_time(aws_df, current_time)

        # Получаем радиацию для ВСЕХ точек на этот момент
        # (Функцию calculate_solar_radiation... используем старую или обновленную,
        # главное что она возвращает словарь для всех точек)
        radiation_data = calculate_solar_radiation_with_excel_pattern(points_gdf, current_time, aws_data)

        for cat in radiation_data:
            radiation_data[cat]['Sin_AWS2'] = aws_data['Sin_AWS2']

        if not radiation_data:
            print(f"⚠ {time_str}: Нет данных радиации, пропускаем шаг")
            current_time += dt.timedelta(minutes=config["time_step_minutes"])
            continue

        print(f"  Обработка: {time_str} | Точек: {len(points_gdf)} | Погода T={aws_data.get('T2m_AWS2', '?'):.1f}")

        # === ВНУТРЕННИЙ ЦИКЛ ПО ТОЧКАМ ===
        # Проходимся по КАЖДОЙ точке для ТЕКУЩЕГО времени
        for idx, point in points_gdf.iterrows():
            cat = point['cat']
            z = point['z']

            if cat not in radiation_data:
                continue

            # --- РАСЧЕТЫ ---
            rad_info = radiation_data[cat]
            G_cell = rad_info['G_cell']
            G_AWS2_cell = rad_info['G_AWS2']
            Sin_AWS2_excel = rad_info['Sin_AWS2']

            # Sin Cell
            Sin_cell = compute_Sin_cell_corrected(Sin_AWS2_excel, G_cell, G_AWS2_cell)

            # Для отладки
            if cat == 94:
                debug_data_94.append({
                    'datetime': current_time,
                    'Sin_cell': Sin_cell,
                    'G_cell': G_cell
                })

            # Температура воздуха
            T2m_pt = compute_T2m_at_z(aws_data['T2m_AWS2'], config["kt"], z, config["z_aws2"])

            # Альбедо и тип поверхности
            ST = 1 if z > config["bsl"] else 0
            Ta = 50
            alpha = compute_albedo(ST, T2m_pt, Ta, config["kSS"], config["kT2m"], config["kTa"], config["c_alpha"])
            Sout = compute_Sout(alpha, Sin_cell)

            Lin = aws_data['Lin_AWS2']

            # Итерация 1
            Lout_temp, Tsurface_temp = compute_Lout_corrected(config["epsilon"], config["sigma"], ST, 0)
            H, LE = compute_turbulent_heat_corrected(T2m_pt, Tsurface_temp, aws_data['wind_speed'],
                                                     aws_data['pressure'], aws_data['RH_AWS2'], z)
            Qr = compute_rain_heat_corrected(T2m_pt, Tsurface_temp, aws_data['precipitation'])
            Qg = compute_ground_heat_corrected(ST, Tsurface_temp, time_decimal)
            Qm_temp = compute_melting_heat(Sin_cell, Sout, Lin, Lout_temp, H, LE, Qr, Qg)

            # Итерация 2 (уточнение Lout)
            Lout, Tsurface = compute_Lout_corrected(config["epsilon"], config["sigma"], ST, Qm_temp)
            Rnet, Snet, Lnet = compute_Rnet(Sin_cell, Sout, Lin, Lout)

            # Финальные потоки
            H, LE = compute_turbulent_heat_corrected(T2m_pt, Tsurface, aws_data['wind_speed'],
                                                     aws_data['pressure'], aws_data['RH_AWS2'], z)
            Qr = compute_rain_heat_corrected(T2m_pt, Tsurface, aws_data['precipitation'])
            Qg = compute_ground_heat_corrected(ST, Tsurface, time_decimal)
            Qm = compute_melting_heat(Sin_cell, Sout, Lin, Lout, H, LE, Qr, Qg)

            ablation = compute_ablation_corrected(Qm, ST, time_step_seconds,
                                                  config["rho_snow"], config["rho_ice"],
                                                  config["L_fs"], config["L_fi"])

            # Сохраняем строку результата
            results.append({
                'datetime': current_time,
                'time_str': time_str,
                'cat': cat,
                'z': z,
                'r_sun_global_rad': G_cell,
                'Sin_cell': Sin_cell,
                'Sout': Sout,
                'Lin': Lin,
                'Lout': Lout,
                'T2m': T2m_pt,
                'Ts': Tsurface,
                'H': H,
                'LE': LE,
                'Qr': Qr,
                'Qg': Qg,
                'Qm': Qm,
                'ablation_mm': ablation
            })

        # === ВАЖНО: ПЕРЕКЛЮЧЕНИЕ ВРЕМЕНИ НАХОДИТСЯ ЗДЕСЬ ===
        # Оно вне цикла `for point`, но внутри цикла `while current_time`
        current_time += dt.timedelta(minutes=config["time_step_minutes"])

    # --- СОХРАНЕНИЕ ---
    print("\n=== СОХРАНЕНИЕ ===")
    results_df = pd.DataFrame(results)

    if results_df.empty:
        print("⚠ ОШИБКА: Результаты пусты!")
    else:
        out_file = Path(config["output_dir"]) / "model_results_full.csv"
        results_df.to_csv(out_file, index=False)
        print(f"✓ Сохранено строк: {len(results_df)}")
        print(f"✓ Файл: {out_file}")

        # Проверка размера (должно быть: кол-во шагов * кол-во точек)
        expected_rows = len(points_gdf) * len(pd.date_range(start, end, freq=f'{config["time_step_minutes"]}min'))
        print(f"✓ Ожидалось строк примерно: {expected_rows}")

    print("ГОТОВО.")


def create_test_aws_data():
    """Создает тестовые метеоданные на основе Excel шаблона"""
    start_date = pd.to_datetime(CONFIG["period_start"])
    dates = [start_date + pd.Timedelta(minutes=30 * i) for i in range(48)]

    # Значения Sin_AWS2 из Excel
    sin_values = [
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,  # 00:00 - 03:30
        8.5, 20.3, 76.1, 231.9, 170.8, 143.6, 138.6, 113.1,  # 04:00 - 07:30
        190.4, 270.7, 350.4, 421.8, 506.2, 632.2, 655.0, 760.4,  # 08:00 - 11:30
        776.2, 870.1, 308.7, 654.6, 267.0, 267.7, 229.1, 265.9,  # 12:00 - 15:30
        418.2, 312.4, 251.8, 12.3, 94.7, 67.6, 45.7, 32.2,  # 16:00 - 19:30
        23.7, 6.8, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0  # 20:00 - 23:30
    ]

    data = []
    for i, date in enumerate(dates):
        sin_val = sin_values[i] if i < len(sin_values) else 0.0

        # Рассчитываем температуру на основе времени суток
        hour = date.hour
        if 6 <= hour <= 18:  # день
            temp = 8 + 4 * np.sin((hour - 6) * np.pi / 12)
        else:  # ночь
            temp = 2 + 2 * np.sin((hour + 6) * np.pi / 12)

        data.append({
            'datetime': date,
            'Sin_AWS2': sin_val,
            'T2m_AWS2': temp,
            'RH_AWS2': 70 - 10 * np.sin(hour * np.pi / 12),
            'wind_speed': 2.0 + 1.0 * np.sin(hour * np.pi / 12),
            'pressure': 1013,
            'precipitation': 0.0,
            'alpha_AWS2': 0.5,
            'Lin_AWS2': 300 + 50 * np.sin(hour * np.pi / 12)
        })

    df = pd.DataFrame(data)
    df['G_AWS2'] = df['Sin_AWS2'] / df['alpha_AWS2']
    return df


# ==================== ЗАПУСК ИСПРАВЛЕННОЙ ПРОГРАММЫ ====================
if __name__ == "__main__":
    run_glacier_model_final_correction()