#!/usr/bin/env python3
"""
ПОЛНАЯ МОДЕЛЬ ЛЕДНИКА С РЕАЛЬНЫМ РАСЧЕТОМ r.sun ЧЕРЕЗ GRASS
"""
import os
import sys
import math
import tempfile
import datetime as dt
from pathlib import Path
import subprocess
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.transform import rowcol
from rasterio.windows import Window

# Просто проверяем наличие GRASS
grass_base = r"C:\Program Files\GRASS GIS 7.8"
grass_bat = os.path.join(grass_base, "grass78.bat")

if not os.path.exists(grass_bat):
    print(f"✗ GRASS не найден по пути: {grass_bat}")
    print("  Установите GRASS GIS 7.8 или измените путь")
    sys.exit(1)

print(f"✓ GRASS найден: {grass_bat}")

# ========== КОНФИГУРАЦИЯ ==========
CONFIG = {
    "dem_tif": "DEM.tif",
    "glacier_shp": "glacier.shp",
    "output_dir": "output_model",
    "time_step_minutes": 30,
    "period_start": "2019-07-07T00:00:00",
    "period_end": "2019-07-07T23:30:00",
    "kt": -0.0065,  # вертикальный градиент температуры
    "asl": 1.7813, "bsl": 2067.6,  # коэффициенты для снеговой линии
    "kSS": 0.33745, "kT2m": 0.00838, "kTa": -0.00112, "c_alpha": 0.13469,  # альбедо
    "rho_ice": 900, "rho_snow": 400,  # плотность льда и снега (кг/м³)
    "sigma": 5.670374419e-8,  # постоянная Стефана-Больцмана
    "epsilon": 1,  # излучательная способность
    "z_aws1": 2540,  # высота AWS1 (морена)
    "z_aws2": 2561,  # высота AWS2 (ледник)
    "L_fs": 334000,  # теплота плавления снега (Дж/кг)
    "L_fi": 334000,  # теплота плавления льда (Дж/кг)
    "alpha_d": 0.06,  # критическая разность альбедо для определения снегопада
    "z0m": 0.001,  # длина шероховатости для момента (м)
    "z0t": 0.0001,  # длина шероховатости для температуры (м)
    "z0h": 0.0001,  # длина шероховатости для влажности (м)
    "zm": 2.0,  # высота измерений (м)
}

def ensure_dir(d):
    """Создает директорию, если её нет"""
    os.makedirs(d, exist_ok=True)


def get_raster_info(raster_path):
    """Получает информацию о растре"""
    with rasterio.open(raster_path) as src:
        return {
            'crs': src.crs,
            'bounds': src.bounds,
            'width': src.width,
            'height': src.height,
            'res': src.res,
            'transform': src.transform,
            'nodata': src.nodata
        }


def extract_point_values_from_raster(raster_path, points_gdf):
    """
    Извлекает значения растра в точках
    """
    values = {}
    with rasterio.open(raster_path) as src:
        for idx, point in points_gdf.iterrows():
            # Конвертируем координаты в индексы растра
            row, col = src.index(point.geometry.x, point.geometry.y)
            window = Window(col, row, 1, 1)
            value = src.read(1, window=window)[0, 0]
            if value != src.nodata and not np.isnan(value):
                values[point['cat']] = float(value)
            else:
                values[point['cat']] = 0.0
    return values


def setup_grass_environment():
    """
    Настраивает временную GRASS сессию и импортирует данные
    """
    print("=== НАСТРОЙКА GRASS СРЕДЫ ===")

    # Создаем временную директорию для GRASS
    gisdb = tempfile.mkdtemp(prefix="grass_")
    location = "glacier_location"
    mapset = "PERMANENT"

    grass_bat = r"C:\Program Files\GRASS GIS 7.8\grass78.bat"  # Тот же путь

    # Проверяем существование
    if not os.path.exists(grass_bat):
        print(f"✗ GRASS не найден: {grass_bat}")
        return None, None, None, None

    print(f"  Используем GRASS: {grass_bat}")
    print(f"  GISDBASE: {gisdb}")

    # Функция для выполнения команд GRASS
    def run_grass_cmd(cmd_list, description=""):
        # Собираем команду
        full_cmd = [grass_bat, "--config", "path", gisdb, location, mapset, "--exec"] + cmd_list

        if description:
            print(f"  {description}...")

        try:
            # Запускаем с таймаутом
            result = subprocess.run(
                full_cmd,
                capture_output=True,
                text=True,
                timeout=120,
                encoding='utf-8',
                errors='ignore'
            )

            if result.returncode != 0:
                if result.stderr and len(result.stderr) > 0:
                    print(f"  ⚠ Ошибка: {result.stderr[:200]}")
                return False

            return True

        except subprocess.TimeoutExpired:
            print(f"  ⚠ Таймаут команды")
            return False
        except Exception as e:
            print(f"  ⚠ Ошибка выполнения: {e}")
            return False

    # Создаем location
    print("  Создание location...")
    create_cmd = [grass_bat, "-c", CONFIG["dem_tif"], "-e", os.path.join(gisdb, location)]
    try:
        result = subprocess.run(create_cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            print(f"  ⚠ Ошибка создания location: {result.stderr}")
            return None, None, None, None
    except Exception as e:
        print(f"  ⚠ Ошибка: {e}")
        return None, None, None, None

    # Импортируем DEM
    if not run_grass_cmd(["r.in.gdal", f"input={CONFIG['dem_tif']}", "output=dem", "--overwrite"],
                         "Импорт DEM"):
        return None, None, None, None

    # Вычисляем slope и aspect
    if not run_grass_cmd(["r.slope.aspect", "elevation=dem", "slope=slope", "aspect=aspect", "--overwrite"],
                         "Вычисление slope/aspect"):
        # Продолжаем, может они уже есть
        print("  ⚠ Не удалось вычислить slope/aspect")

    # Импортируем границы ледника
    if not run_grass_cmd(["v.in.ogr", f"input={CONFIG['glacier_shp']}", "output=glacier", "--overwrite"],
                         "Импорт границ ледника"):
        print("  ⚠ Не удалось импортировать границы ледника")

    # Устанавливаем регион
    if not run_grass_cmd(["g.region", "raster=dem"], "Установка региона"):
        print("  ⚠ Не удалось установить регион")

    print("✓ GRASS среда настроена")
    return gisdb, location, mapset, grass_bat


def calculate_rsun_for_time(gisdb, location, mapset, grass_bat,
                            points_gdf, target_datetime):
    """
    Запускает r.sun для конкретного времени и возвращает значения G(z,t) для всех точек
    """
    # Вычисляем день года и время
    day_of_year = target_datetime.timetuple().tm_yday
    time_decimal = target_datetime.hour + target_datetime.minute / 60.0

    print(f"\n  r.sun расчет: день {day_of_year}, время {time_decimal:.2f}")

    # Уникальное имя для выходного растра
    rad_name = f"glob_rad_{day_of_year}_{int(time_decimal * 100):03d}"

    # Функция для выполнения команд GRASS
    def run_grass_cmd(cmd_list, timeout=180):
        full_cmd = [
                       grass_bat,
                       "--config", "path", gisdb, location, mapset,
                       "--exec"
                   ] + cmd_list
        try:
            result = subprocess.run(
                full_cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                encoding='utf-8',
                errors='ignore'
            )
            if result.returncode != 0:
                print(f"    ⚠ STDERR: {result.stderr[:200] if result.stderr else 'None'}")
            return result.returncode == 0, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            print(f"    ⚠ Таймаут команды")
            return False, "", "Timeout"
        except Exception as e:
            print(f"    ⚠ Ошибка: {e}")
            return False, "", str(e)

    # Проверяем, есть ли уже slope и aspect
    print("  Проверка наличия slope и aspect...")
    check_cmd = ["g.list", "type=raster", "pattern=slope*"]
    success, stdout, stderr = run_grass_cmd(check_cmd)
    if not success or "slope" not in stdout:
        print("  Вычисляем slope и aspect...")
        slope_cmd = ["r.slope.aspect", "elevation=dem", "slope=slope", "aspect=aspect", "--overwrite"]
        run_grass_cmd(slope_cmd)

    # Запускаем r.sun
    print(f"  Запуск r.sun для {len(points_gdf)} точек...")
    rsun_cmd = [
        "r.sun",
        "elevation=dem",
        "slope=slope",
        "aspect=aspect",
        f"day={day_of_year}",
        f"time={time_decimal:.2f}",
        f"glob_rad={rad_name}",
        "--overwrite"
    ]

    success, stdout, stderr = run_grass_cmd(rsun_cmd, timeout=300)

    if not success:
        print(f"  ✗ r.sun не удался")
        return {}

    print(f"  ✓ r.sun выполнен успешно")

    # Пробуем несколько способов получить значения

    G_values = {}

    # СПОСОБ 1: Прямой экспорт растра и чтение через rasterio
    print("  Способ 1: Прямой экспорт растра...")
    temp_tif = tempfile.NamedTemporaryFile(suffix='.tif', delete=False)
    temp_tif.close()

    export_cmd = ["r.out.gdal", f"input={rad_name}", f"output={temp_tif.name}", "format=GTiff", "--overwrite", "-c"]
    success, stdout, stderr = run_grass_cmd(export_cmd)

    if success and os.path.exists(temp_tif.name) and os.path.getsize(temp_tif.name) > 0:
        try:
            with rasterio.open(temp_tif.name) as src:
                for idx, point in points_gdf.iterrows():
                    try:
                        # Конвертируем координаты в индексы растра
                        row, col = src.index(point.geometry.x, point.geometry.y)

                        # Проверяем, что индексы в пределах растра
                        if 0 <= row < src.height and 0 <= col < src.width:
                            window = Window(col, row, 1, 1)
                            val = src.read(1, window=window)[0, 0]
                            if val != src.nodata and not np.isnan(val) and val > 0:
                                G_values[point['cat']] = float(val)
                    except Exception as e:
                        continue

            if G_values:
                print(f"    Получено {len(G_values)} значений через экспорт")
        except Exception as e:
            print(f"    Ошибка чтения экспортированного растра: {e}")

    # Очистка временного файла
    if os.path.exists(temp_tif.name):
        os.unlink(temp_tif.name)

    # СПОСОБ 2: Если не получили значения, пробуем r.stats
    if not G_values:
        print("  Способ 2: Использование r.stats...")
        stats_cmd = ["r.stats", f"map={rad_name}", "flags=1n", "separator=comma"]
        success, stdout, stderr = run_grass_cmd(stats_cmd)

        if success and stdout:
            # Создаем словарь для быстрого поиска по координатам
            stats_dict = {}
            for line in stdout.strip().split('\n'):
                if line and ',' in line:
                    parts = line.split(',')
                    if len(parts) >= 4:
                        try:
                            x = float(parts[0])
                            y = float(parts[1])
                            val = float(parts[3])
                            stats_dict[(round(x, 1), round(y, 1))] = val
                        except:
                            pass

            # Ищем ближайшие точки
            for idx, point in points_gdf.iterrows():
                px, py = point.geometry.x, point.geometry.y
                # Ищем точное или ближайшее совпадение
                found = False
                for (rx, ry), val in stats_dict.items():
                    if abs(rx - px) < 50 and abs(ry - py) < 50:  # в пределах половины ячейки
                        G_values[point['cat']] = val
                        found = True
                        break

                if not found:
                    # Берем среднее значение
                    if stats_dict:
                        G_values[point['cat']] = sum(stats_dict.values()) / len(stats_dict)

            print(f"    Получено {len(G_values)} значений через r.stats")

    # СПОСОБ 3: Если всё ещё нет значений, используем среднее по рару
    if not G_values:
        print("  Способ 3: Использование среднего значения...")
        univar_cmd = ["r.univar", f"map={rad_name}", "flags=g"]
        success, stdout, stderr = run_grass_cmd(univar_cmd)

        mean_val = 500  # значение по умолчанию
        if success and stdout:
            for line in stdout.split('\n'):
                if 'mean:' in line:
                    try:
                        mean_val = float(line.split(':')[1].strip())
                        break
                    except:
                        pass

        print(f"    Среднее значение: {mean_val:.1f}")

        # Используем среднее для всех точек
        for idx, point in points_gdf.iterrows():
            # Добавляем небольшую вариацию по высоте
            height_factor = 1.0 + (point['z'] - 2500) / 1000 * 0.1
            G_values[point['cat']] = mean_val * height_factor

        print(f"    Создано {len(G_values)} аппроксимированных значений")

    # Очистка растра
    cleanup_cmd = ["g.remove", "-f", "type=raster", f"name={rad_name}"]
    run_grass_cmd(cleanup_cmd)

    return G_values


def load_aws_data(excel_file="Test_model.xlsx"):
    """
    Загружает метеоданные с AWS2
    """
    print(f"Загрузка метеоданных из {excel_file}...")

    try:
        # Пытаемся загрузить с правильным форматом
        df = pd.read_excel(excel_file, sheet_name="AWS2_30min", header=2)

        # Переименовываем столбцы
        column_mapping = {
            'X': 'x',
            'Y': 'y',
            'Z': 'z',
            'Sin': 'Sin',
            'Sout': 'Sout',
            'Lin': 'Lin',
            'T2m': 'T2m',
            'RH2m': 'RH',
            'W2m': 'wind_speed',
            'p': 'pressure',
            'Prec': 'precipitation',
            'α': 'alpha'
        }

        # Применяем переименование только для существующих столбцов
        rename_dict = {old: new for old, new in column_mapping.items() if old in df.columns}
        df = df.rename(columns=rename_dict)

        # Создаем временные метки
        start_date = pd.to_datetime(CONFIG["period_start"])
        df['datetime'] = [start_date + pd.Timedelta(minutes=30 * i) for i in range(len(df))]

        print(f"✓ Загружено {len(df)} записей")
        return df

    except Exception as e:
        print(f"✗ Ошибка загрузки: {e}")
        # Создаем тестовые данные
        return create_test_aws_data()


def create_test_aws_data():
    """Создает тестовые метеоданные для отладки"""
    print("Создание тестовых метеоданных...")

    start = pd.to_datetime(CONFIG["period_start"])
    end = pd.to_datetime(CONFIG["period_end"])
    dates = pd.date_range(start, end, freq='30min')

    data = []
    for date in dates:
        hour = date.hour
        # Синусоидальная дневная температура
        if 6 <= hour <= 18:
            temp = 8 + 6 * np.sin((hour - 6) * np.pi / 12)
        else:
            temp = 2

        # Радиация (только днем)
        if 4 <= hour <= 20:
            sin_val = 500 * np.sin((hour - 4) * np.pi / 16)
        else:
            sin_val = 0

        data.append({
            'datetime': date,
            'Sin': max(0, sin_val),
            'Sout': max(0, sin_val * 0.3),
            'Lin': 300,
            'T2m': temp,
            'RH': 70,
            'wind_speed': 3,
            'pressure': 850,
            'precipitation': 0,
            'alpha': 0.5
        })

    return pd.DataFrame(data)


def get_aws_at_time(aws_df, target_datetime):
    """
    Получает метеоданные для конкретного времени
    """
    mask = aws_df['datetime'] == target_datetime
    if mask.any():
        return aws_df[mask].iloc[0].to_dict()
    else:
        # Интерполяция по времени
        time_diff = abs(aws_df['datetime'] - target_datetime)
        closest_idx = time_diff.idxmin()
        return aws_df.loc[closest_idx].to_dict()


def compute_T2m_at_z(T2m_aws2, kt, z_cell, z_aws2):
    """Температура воздуха на высоте ячейки (формула 11)"""
    return T2m_aws2 + kt * (z_cell - z_aws2)


def compute_Sin_cell(Sin_aws2, G_cell, G_aws2):
    """Приходящая коротковолновая радиация в ячейке (формула 1)"""
    if G_aws2 <= 0 or Sin_aws2 <= 0:
        return 0.0
    return Sin_aws2 * (G_cell / G_aws2)


def compute_albedo(ST, T2m, Ta, kSS, kT2m, kTa, c_alpha):
    """Альбедо поверхности (формула 7)"""
    albedo = kSS * ST + kT2m * T2m + kTa * Ta + c_alpha
    return max(0.1, min(0.9, albedo))


def compute_Sout(alpha, Sin):
    """Отраженная коротковолновая радиация (формула 8)"""
    return alpha * Sin


def compute_Lout(epsilon, sigma, Qm, ST):
    """Длинноволновое излучение поверхности (формула 10)"""
    if Qm > 0:
        Ts_K = 273.15  # 0°C при таянии
    else:
        if ST == 1:  # снег
            Ts_K = 271.15  # -2°C
        else:  # лед
            Ts_K = 272.15  # -1°C

    return epsilon * sigma * (Ts_K ** 4), Ts_K - 273.15


def compute_vapor_pressure(T2m, RH, p):
    """Давление водяного пара (формула 15)"""
    T_C = T2m
    term1 = 6.112 * math.exp(17.62 * T_C / (243.12 + T_C))
    term2 = 1.0016 + 0.0000315 * p - 0.074 / p
    return term1 * term2 * (RH / 100)


def compute_richardson(T2m, Ts, wind_speed, zm=2.0, z0m=0.001):
    """Число Ричардсона (формула 16)"""
    if wind_speed <= 0.5:
        return None

    T2m_K = T2m + 273.15
    Ts_K = Ts + 273.15
    delta_T = T2m - Ts
    g = 9.81

    Rib = g * delta_T * (zm - z0m) / (T2m_K * wind_speed ** 2)
    return Rib


def compute_phi_functions(Rib):
    """Безразмерные функции (формула 17)"""
    Rib_cr = 0.4

    if abs(Rib) >= Rib_cr or Rib is None:
        return 0

    if Rib > 0:  # стабильные
        return (1 - 5 * Rib) ** 2
    else:  # нестабильные
        return (1 - 16 * Rib) ** 0.75


def compute_turbulent_heat(T2m, Ts, wind_speed, pressure, RH, Rib,
                           p0=1013.25, rho0=1.225, cp=1005, k=0.4,
                           Lv=2.83e6, es=6.11, zm=2.0, z0m=0.001,
                           z0t=0.0001, z0h=0.0001):
    """Явное (H) и скрытое (LE) тепло (формулы 18, 19)"""
    if wind_speed <= 0.5 or Rib is None or Rib >= 0.4:
        return 0, 0

    phi_inv = compute_phi_functions(Rib)
    if phi_inv == 0:
        return 0, 0

    # Явное тепло H
    delta_T = T2m - Ts
    H = (cp * rho0 * (pressure / p0) * (k ** 2) * wind_speed * delta_T *
         phi_inv / (math.log(zm / z0m) * math.log(zm / z0t)))

    # Скрытое тепло LE
    e_air = compute_vapor_pressure(T2m, RH, pressure)
    delta_e = e_air - es
    LE = (0.623 * Lv * rho0 * (1 / p0) * (k ** 2) * wind_speed * delta_e *
          phi_inv / (math.log(zm / z0m) * math.log(zm / z0h)))

    return H, LE


def compute_rain_heat(T2m, Ts, precipitation):
    """Тепло от жидких осадков (формула 20)"""
    if T2m < 2 or precipitation <= 0:
        return 0

    rho_w = 1000  # кг/м³
    cw = 4186  # Дж/(кг·K)

    # Конвертация мм/шаг в м/с
    precip_ms = precipitation / 1000 / 1800  # за 30 мин

    return rho_w * cw * precip_ms * (T2m - Ts)


def compute_ground_heat(ST, Ts):
    """Теплообмен с ледником (формула 21)"""
    if ST == 1:  # снег
        k_r = 0.2
        T_g = 271.15  # -2°C
    else:  # лед
        k_r = 2.2
        T_g = 272.15  # -1°C

    z_g = 0.1  # глубина (м)
    z_0 = 0.01  # толщина поверхностного слоя (м)

    Ts_K = Ts + 273.15
    return -k_r * (T_g - Ts_K) / (z_g - z_0)


def compute_melting_heat(Sin, Sout, Lin, Lout, H, LE, Qr, Qg):
    """Тепло на таяние (формула 22)"""
    Qm = Sin - Sout + Lin - Lout + H + LE + Qr + Qg
    return max(0, Qm)


def compute_ablation(Qm, ST, time_step=1800, L_fs=334000, L_fi=334000):
    """Абляция в мм в.э. (формула 23)"""
    if Qm <= 0:
        return 0

    L_f = L_fs if ST == 1 else L_fi
    rho_w = 1000  # кг/м³

    # Энергия таяния за шаг (Дж/м²)
    energy = Qm * time_step

    # Масса растаявшего вещества (кг/м²)
    mass = energy / L_f

    # Объем воды (м³/м²)
    volume = mass / rho_w

    # в мм
    return volume * 1000


def check_glacier_overlap(dem_tif, glacier_shp):
    """
    Проверяет перекрытие DEM и границ ледника
    """
    print("\n=== ПРОВЕРКА ПЕРЕКРЫТИЯ DEM И ЛЕДНИКА ===")

    # Открываем DEM
    src = rasterio.open(dem_tif)
    print(f"DEM CRS: {src.crs}")
    print(f"DEM bounds: {src.bounds}")
    print(f"DEM размер: {src.width} x {src.height}")
    print(f"DEM ячеек: {src.width * src.height}")

    # Читаем небольшой участок DEM для проверки
    dem_data = src.read(1)
    valid_cells = np.sum((dem_data > -999) & (dem_data < 10000))
    print(f"Ячеек с валидными высотами: {valid_cells}")

    # Проверяем шейп-файл
    glacier_gdf = gpd.read_file(glacier_shp)
    print(f"\nGlacier CRS: {glacier_gdf.crs}")
    print(f"Glacier bounds: {glacier_gdf.total_bounds}")
    print(f"Количество полигонов: {len(glacier_gdf)}")

    # Если CRS разные, пробуем преобразовать
    if glacier_gdf.crs != src.crs:
        print(f"\n⚠ CRS различаются!")
        print(f"Преобразуем glacier в CRS DEM...")
        glacier_gdf = glacier_gdf.to_crs(src.crs)
        print(f"После преобразования bounds: {glacier_gdf.total_bounds}")

    # Проверяем перекрытие
    dem_bounds = src.bounds
    glacier_bounds = glacier_gdf.total_bounds

    overlap_x = not (glacier_bounds[2] < dem_bounds[0] or glacier_bounds[0] > dem_bounds[2])
    overlap_y = not (glacier_bounds[3] < dem_bounds[1] or glacier_bounds[1] > dem_bounds[3])

    if overlap_x and overlap_y:
        print("\n✅ DEM и ледник перекрываются!")

        # Создаем пробную точку в центре ледника
        center_x = (glacier_bounds[0] + glacier_bounds[2]) / 2
        center_y = (glacier_bounds[1] + glacier_bounds[3]) / 2
        test_point = gpd.points_from_xy([center_x], [center_y])[0]

        # Проверяем каждую часть мультиполигона
        point_inside = False
        for geom in glacier_gdf.geometry:
            if geom.contains(test_point):
                point_inside = True
                break

        if point_inside:
            print("✅ Центр ледника находится внутри полигона")
        else:
            print("❌ Центр ледника НЕ внутри полигона")
            print("  Возможно, ледник состоит из нескольких отдельных полигонов")

    else:
        print("\n❌ DEM и ледник НЕ перекрываются!")
        print(f"DEM X: [{dem_bounds[0]:.0f}, {dem_bounds[2]:.0f}]")
        print(f"DEM Y: [{dem_bounds[1]:.0f}, {dem_bounds[3]:.0f}]")
        print(f"Glacier X: [{glacier_bounds[0]:.0f}, {glacier_bounds[2]:.0f}]")
        print(f"Glacier Y: [{glacier_bounds[1]:.0f}, {glacier_bounds[3]:.0f}]")

    return glacier_gdf, src, dem_data  # Возвращаем всё, включая открытый src и данные


def create_research_points(dem_tif, glacier_shp, num_points=100):
    """
    Создает точки для исследования с улучшенной диагностикой
    """
    print(f"\nСоздание {num_points} точек на леднике...")

    # Проверяем перекрытие и получаем открытый файл
    glacier_gdf, src, dem_data = check_glacier_overlap(dem_tif, glacier_shp)

    points = []

    # Стратегия 1: ищем по всему DEM
    print("\nСтратегия 1: Поиск по всему DEM...")
    found = 0

    # Получаем bounding box ледника для ускорения
    bounds = glacier_gdf.total_bounds
    minx, miny, maxx, maxy = bounds

    # Преобразуем границы в индексы растра
    try:
        row_min, col_min = src.index(minx, maxy)  # верхний левый
        row_max, col_max = src.index(maxx, miny)  # нижний правый

        # Нормализуем индексы
        row_start = max(0, min(row_min, row_max))
        row_end = min(src.height, max(row_min, row_max) + 1)
        col_start = max(0, min(col_min, col_max))
        col_end = min(src.width, max(col_min, col_max) + 1)

        print(f"  Область поиска по bounding box: строки {row_start}-{row_end - 1}, колонки {col_start}-{col_end - 1}")
    except:
        # Если не удалось преобразовать, ищем по всему DEM
        row_start, row_end = 0, src.height
        col_start, col_end = 0, src.width
        print(f"  Область поиска: весь DEM")

    for j in range(row_start, row_end):
        for i in range(col_start, col_end):
            # Пропускаем невалидные ячейки
            if dem_data[j, i] <= src.nodata or dem_data[j, i] < -999 or dem_data[j, i] > 10000:
                continue

            # Получаем координаты центра ячейки
            x, y = src.xy(j, i)

            # Проверяем, попадает ли точка в ледник
            point_geom = gpd.points_from_xy([x], [y])[0]

            try:
                # Проверяем по всем полигонам
                for geom in glacier_gdf.geometry:
                    if geom.contains(point_geom):
                        cat = len(points) + 1
                        points.append({
                            'cat': cat,
                            'x': x,
                            'y': y,
                            'z': float(dem_data[j, i]),
                            'row': j,
                            'col': i,
                            'geometry': point_geom
                        })
                        found += 1
                        break  # Выходим из цикла по полигонам

                if len(points) >= num_points:
                    break
            except Exception as e:
                continue

        if len(points) >= num_points:
            break

    print(f"  Найдено {found} точек внутри ледника")

    # Если не нашли точек, выводим отладочную информацию
    if found == 0:
        print("\n❌ НЕ НАЙДЕНО ТОЧЕК ВНУТРИ ЛЕДНИКА!")
        print("\nОтладочная информация:")

        # Проверяем первые несколько ячеек в bounding box
        print(f"\nПроверка первых 10 ячеек в области поиска:")
        check_count = 0
        for j in range(row_start, min(row_start + 5, row_end)):
            for i in range(col_start, min(col_start + 5, col_end)):
                if j < src.height and i < src.width:
                    x, y = src.xy(j, i)
                    z = dem_data[j, i]
                    point_geom = gpd.points_from_xy([x], [y])[0]

                    inside = False
                    for geom in glacier_gdf.geometry:
                        if geom.contains(point_geom):
                            inside = True
                            break

                    print(f"    Ячейка [{j},{i}]: x={x:.0f}, y={y:.0f}, z={z:.1f}, внутри ледника: {inside}")
                    check_count += 1

        # Если ни одной точки не внутри, возможно проблема с ориентацией полигона
        print("\nВозможные причины:")
        print("1. Полигоны в шейп-файле ориентированы неправильно (self-intersection)")
        print("2. В шейп-файле мультиполигон с дырками")
        print("3. Координаты точек находятся на границе, а contains требует строгого внутри")

        # Пробуем использовать within вместо contains
        print("\nСтратегия 2: Использование within вместо contains...")
        for j in range(row_start, row_end):
            for i in range(col_start, col_end):
                if dem_data[j, i] <= src.nodata:
                    continue

                x, y = src.xy(j, i)
                point_geom = gpd.points_from_xy([x], [y])[0]

                # Пробуем within
                if point_geom.within(glacier_gdf.geometry.iloc[0]):
                    cat = len(points) + 1
                    points.append({
                        'cat': cat,
                        'x': x,
                        'y': y,
                        'z': float(dem_data[j, i]),
                        'row': j,
                        'col': i,
                        'geometry': point_geom
                    })
                    if len(points) >= num_points:
                        break
            if len(points) >= num_points:
                break

        if points:
            print(f"  Найдено {len(points)} точек через within!")

    # Если всё равно нет точек, создаем тестовые
    if not points:
        print("\nСоздаем тестовые точки на основе bounding box ледника...")

        # Берем точки из bounding box
        x_step = (maxx - minx) / 10
        y_step = (maxy - miny) / 10

        for ix in range(10):
            for iy in range(10):
                test_x = minx + ix * x_step + x_step / 2
                test_y = miny + iy * y_step + y_step / 2

                try:
                    row, col = src.index(test_x, test_y)
                    if 0 <= row < src.height and 0 <= col < src.width:
                        z = float(dem_data[row, col])
                        if z > -999 and z < 10000:
                            point_geom = gpd.points_from_xy([test_x], [test_y])[0]
                            points.append({
                                'cat': len(points) + 1,
                                'x': test_x,
                                'y': test_y,
                                'z': z,
                                'row': row,
                                'col': col,
                                'geometry': point_geom
                            })
                except:
                    continue

                if len(points) >= num_points:
                    break
            if len(points) >= num_points:
                break

        print(f"  Создано {len(points)} тестовых точек")

    # Создаем GeoDataFrame
    if points:
        points_gdf = gpd.GeoDataFrame(points, crs=src.crs, geometry='geometry')
        print(f"\n✅ ИТОГО создано точек: {len(points_gdf)}")
        print(f"  Диапазон высот: {points_gdf['z'].min():.1f} - {points_gdf['z'].max():.1f} м")
        print(f"  Диапазон X: {points_gdf['x'].min():.0f} - {points_gdf['x'].max():.0f}")
        print(f"  Диапазон Y: {points_gdf['y'].min():.0f} - {points_gdf['y'].max():.0f}")

        # Закрываем DEM
        src.close()
        return points_gdf
    else:
        print("\n❌ НЕ УДАЛОСЬ СОЗДАТЬ ТОЧКИ!")
        src.close()
        return gpd.GeoDataFrame(columns=['cat', 'x', 'y', 'z', 'row', 'col', 'geometry'], crs=src.crs)

def run_glacier_model():
    """
    Основная функция модели
    """
    print("=" * 70)
    print("ЗАПУСК МОДЕЛИ АБЛЯЦИИ ЛЕДНИКА")
    print("=" * 70)

    # Создаем выходную директорию
    ensure_dir(CONFIG["output_dir"])

    # Настраиваем GRASS
    gisdb, location, mapset, grass_bat = setup_grass_environment()
    if not gisdb:
        print("✗ Не удалось настроить GRASS. Выход.")
        return

    # Загружаем метеоданные
    aws_df = load_aws_data()

    if not os.path.exists(CONFIG["dem_tif"]):
        print(f"✗ DEM файл не найден: {CONFIG['dem_tif']}")
        return

    if not os.path.exists(CONFIG["glacier_shp"]):
        print(f"✗ Shapefile не найден: {CONFIG['glacier_shp']}")
        return

    # Создаем точки
    points_gdf = create_research_points(CONFIG["dem_tif"], CONFIG["glacier_shp"])
    if points_gdf.empty:
        print("✗ Не удалось создать точки. Выход.")
        return

    # Словарь для хранения дневных переменных
    daily_alpha = {}  # альбедо в полдень
    daily_SD = {}  # снегопад
    daily_Zsl = {}  # высота снеговой линии
    daily_Ta_sum = {}  # сумма температур после снегопада
    daily_days_since_snow = {}  # дней после снегопада

    # Временной диапазон
    start = pd.to_datetime(CONFIG["period_start"])
    end = pd.to_datetime(CONFIG["period_end"])
    time_step_seconds = CONFIG["time_step_minutes"] * 60

    # Список для результатов
    results = []

    # Текущее время
    current_time = start

    print("\n=== НАЧАЛО РАСЧЕТОВ ===")

    while current_time <= end:
        time_str = current_time.strftime("%Y-%m-%d %H:%M")
        current_date = current_time.date()

        # Получаем метеоданные для этого времени
        aws_row = get_aws_at_time(aws_df, current_time)

        # --- ДНЕВНЫЕ РАСЧЕТЫ (выполняются один раз в день в 12:00) ---
        if current_time.hour == 12 and current_time.minute == 0:
            date_key = current_date

            # Получаем альбедо в 12:00
            alpha_12h = aws_row.get('alpha', 0.5)
            daily_alpha[date_key] = alpha_12h

            # Проверяем предыдущий день
            prev_date = current_date - dt.timedelta(days=1)
            if prev_date in daily_alpha:
                alpha_prev = daily_alpha[prev_date]
                alpha_diff = alpha_12h - alpha_prev

                # Определяем день со снегопадом (формула 2)
                daily_SD[date_key] = 1 if alpha_diff >= CONFIG["alpha_d"] else 0

                # Обновляем сумму температур после снегопада
                if daily_SD[date_key] == 1:
                    daily_days_since_snow[date_key] = 0
                    daily_Ta_sum[date_key] = aws_row.get('T2m', 0)
                else:
                    if prev_date in daily_days_since_snow:
                        daily_days_since_snow[date_key] = daily_days_since_snow[prev_date] + 1
                        if prev_date in daily_Ta_sum:
                            daily_Ta_sum[date_key] = daily_Ta_sum[prev_date] + aws_row.get('T2m', 0)
                        else:
                            daily_Ta_sum[date_key] = aws_row.get('T2m', 0)
                    else:
                        daily_days_since_snow[date_key] = 0
                        daily_Ta_sum[date_key] = aws_row.get('T2m', 0)
            else:
                daily_SD[date_key] = 0
                daily_days_since_snow[date_key] = 0
                daily_Ta_sum[date_key] = aws_row.get('T2m', 0)

            # Высота снеговой линии (формула 3)
            day_of_year = current_time.timetuple().tm_yday
            daily_Zsl[date_key] = CONFIG["asl"] * day_of_year + CONFIG["bsl"]

        # --- РАСЧЕТ r.sun ДЛЯ ВСЕХ ТОЧЕК ---
        print(f"\n{time_str}: расчет r.sun...")
        G_values = calculate_rsun_for_time(
            gisdb, location, mapset, grass_bat,
            points_gdf, current_time
        )

        if not G_values:
            print(f"  ⚠ Нет данных r.sun для {time_str}, пропускаем")
            current_time += dt.timedelta(minutes=CONFIG["time_step_minutes"])
            continue

        # G для AWS2 (точка с минимальной высотой)
        aws2_point = points_gdf.iloc[0]  # или найти по координатам
        G_aws2 = G_values.get(aws2_point['cat'], 887.7)

        # Sin AWS2 из метеоданных
        Sin_aws2 = aws_row.get('Sin', 0)

        print(f"  G_AWS2={G_aws2:.1f}, Sin_AWS2={Sin_aws2:.1f}")

        # --- РАСЧЕТ ДЛЯ КАЖДОЙ ТОЧКИ ---
        for idx, point in points_gdf.iterrows():
            cat = point['cat']
            z = point['z']

            if cat not in G_values:
                continue

            G_cell = G_values[cat]

            # 1. Приходящая КВ-радиация (формула 1)
            Sin_cell = compute_Sin_cell(Sin_aws2, G_cell, G_aws2)

            # 2. Температура воздуха (формула 11)
            T2m_pt = compute_T2m_at_z(
                aws_row.get('T2m', 0),
                CONFIG["kt"],
                z,
                CONFIG["z_aws2"]
            )

            # 3. Тип поверхности (формула 4)
            date_key = current_date
            SD = daily_SD.get(date_key, 0)
            Zsl = daily_Zsl.get(date_key, 9999)

            if SD == 1 or z >= Zsl:
                ST = 1  # снег
            else:
                ST = 0  # лед

            # 4. Сумма температур (формула 6)
            nd_aws2 = daily_days_since_snow.get(date_key, 0)
            Ta_aws2 = daily_Ta_sum.get(date_key, T2m_pt)
            Ta_pt = Ta_aws2 + (nd_aws2 + 1) * CONFIG["kt"] * (z - CONFIG["z_aws2"])

            # 5. Альбедо (формула 7)
            alpha = compute_albedo(
                ST, T2m_pt, Ta_pt,
                CONFIG["kSS"], CONFIG["kT2m"], CONFIG["kTa"],
                CONFIG["c_alpha"]
            )

            # 6. Отраженная радиация (формула 8)
            Sout = compute_Sout(alpha, Sin_cell)

            # 7. Длинноволновая радиация атмосферы (формула 9)
            Lin = aws_row.get('Lin', 300)

            # Первая итерация для Lout (без таяния)
            Lout_temp, Ts_temp = compute_Lout(
                CONFIG["epsilon"],
                CONFIG["sigma"],
                0,  # Qm=0 для первой итерации
                ST
            )

            # 8. Число Ричардсона (формула 16)
            Rib = compute_richardson(
                T2m_pt, Ts_temp,
                aws_row.get('wind_speed', 2),
                CONFIG["zm"], CONFIG["z0m"]
            )

            # 9. Турбулентные потоки (формулы 18, 19)
            H, LE = compute_turbulent_heat(
                T2m_pt, Ts_temp,
                aws_row.get('wind_speed', 2),
                aws_row.get('pressure', 850),
                aws_row.get('RH', 70),
                Rib
            )

            # 10. Тепло от осадков (формула 20)
            Qr = compute_rain_heat(
                T2m_pt, Ts_temp,
                aws_row.get('precipitation', 0)
            )

            # 11. Теплообмен с ледником (формула 21)
            Qg = compute_ground_heat(ST, Ts_temp)

            # 12. Тепло на таяние (первая оценка)
            Qm_temp = compute_melting_heat(
                Sin_cell, Sout, Lin, Lout_temp,
                H, LE, Qr, Qg
            )

            # 13. Финальный Lout с учетом таяния (формула 10)
            Lout, Ts = compute_Lout(
                CONFIG["epsilon"],
                CONFIG["sigma"],
                Qm_temp,
                ST
            )

            # 14. Финальные потоки
            H, LE = compute_turbulent_heat(
                T2m_pt, Ts,
                aws_row.get('wind_speed', 2),
                aws_row.get('pressure', 850),
                aws_row.get('RH', 70),
                Rib
            )

            Qg = compute_ground_heat(ST, Ts)

            Qm = compute_melting_heat(
                Sin_cell, Sout, Lin, Lout,
                H, LE, Qr, Qg
            )

            # 15. Абляция (формула 23)
            ablation = compute_ablation(
                Qm, ST, time_step_seconds,
                CONFIG["L_fs"], CONFIG["L_fi"]
            )

            # Радиационный баланс
            Snet = Sin_cell - Sout
            Lnet = Lin - Lout
            Rnet = Snet + Lnet

            # Сохраняем результат
            results.append({
                'datetime': current_time,
                'date': current_time.date(),
                'time': current_time.time(),
                'cat': cat,
                'z': z,
                'G_rsun': G_cell,
                'Sin': Sin_cell,
                'alpha': alpha,
                'Sout': Sout,
                'Snet': Snet,
                'Lin': Lin,
                'Lout': Lout,
                'Lnet': Lnet,
                'Rnet': Rnet,
                'T2m': T2m_pt,
                'Ts': Ts,
                'H': H,
                'LE': LE,
                'Qt': Qr,
                'Qg': Qg,
                'Qm': Qm,
                'ablation_mm': ablation,
                'ST': ST,
                'Rib': Rib if Rib is not None else 0
            })

        # Переходим к следующему шагу
        current_time += dt.timedelta(minutes=CONFIG["time_step_minutes"])

    # --- СОХРАНЕНИЕ РЕЗУЛЬТАТОВ ---
    print("\n=== СОХРАНЕНИЕ РЕЗУЛЬТАТОВ ===")

    if not results:
        print("✗ Нет результатов для сохранения")
        return

    results_df = pd.DataFrame(results)

    # Сохраняем в CSV
    csv_file = Path(CONFIG["output_dir"]) / "model_results_complete.csv"
    results_df.to_csv(csv_file, index=False)
    print(f"✓ CSV сохранен: {csv_file}")
    print(f"  Строк: {len(results_df)}")
    print(f"  Уникальных точек: {results_df['cat'].nunique()}")
    print(f"  Временных шагов: {results_df['datetime'].nunique()}")

    # Создаем сводку по точкам
    pivot_file = Path(CONFIG["output_dir"]) / "points_summary.csv"
    points_summary = results_df.groupby('cat').agg({
        'z': 'first',
        'ablation_mm': 'sum',
        'Qm': 'mean',
        'Sin': 'mean'
    }).round(2)
    points_summary.to_csv(pivot_file)
    print(f"✓ Сводка по точкам: {pivot_file}")

    # Сохраняем пример данных для верификации
    sample = results_df[results_df['cat'] == 1].head(48)
    sample_file = Path(CONFIG["output_dir"]) / "sample_point_1.csv"
    sample.to_csv(sample_file, index=False)

    print("\n=== МОДЕЛЬ ЗАВЕРШЕНА ===")

    # Очистка временной GRASS директории
    try:
        import shutil
        shutil.rmtree(gisdb)
        print(f"✓ Временная GRASS директория удалена: {gisdb}")
    except:
        print(f"⚠ Не удалось удалить {gisdb}")


if __name__ == "__main__":
    run_glacier_model()