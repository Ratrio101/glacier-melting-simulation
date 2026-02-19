import pandas as pd


def diagnose_model_sheet(excel_file="../test_model.xlsx"):
    """
    Диагностика листа model_30min для понимания структуры данных
    """
    print("=" * 60)
    print("ДИАГНОСТИКА ЛИСТА model_30min")
    print("=" * 60)

    # 1. Читаем RAW
    df_raw = pd.read_excel(excel_file, sheet_name="Model_30min", header=None, nrows=10)
    print(f"\nПервые 10 строк RAW:")
    for i in range(min(10, len(df_raw))):
        row_vals = [str(v)[:15] for v in df_raw.iloc[i].tolist()]
        print(f"  Строка {i}: {row_vals[:20]}")  # первые 20 столбцов

    print(f"\nВсего столбцов: {df_raw.shape[1]}")

    # 2. Определяем заголовок
    header_row = None
    for i in range(min(5, len(df_raw))):
        row_str = ' '.join([str(v) for v in df_raw.iloc[i].tolist()])
        if any(key in row_str for key in ['cat', 'Sin', 'G', 'r.sun', 'Sout']):
            header_row = i
            print(f"\n✓ Заголовок в строке {i}")
            break

    if header_row is None:
        header_row = 0
        print(f"\n⚠ Заголовок не найден, пробуем строку 0")

    # 3. Читаем с заголовком
    df = pd.read_excel(excel_file, sheet_name="Model_30min", header=header_row)
    print(f"\nСтолбцы: {df.columns.tolist()}")
    print(f"Количество строк: {len(df)}")
    print(f"\nПервые 5 строк:")
    print(df.head().to_string())

    # 4. Ищем столбец с cat/номером точки
    print(f"\n=== ПОИСК КЛЮЧЕВЫХ СТОЛБЦОВ ===")
    for col in df.columns:
        col_str = str(col).strip().lower()
        if col_str in ['cat', 'id', 'point', 'cell', 'ячейка', '№', 'n']:
            print(f"  Столбец номера точки: '{col}', уникальных: {df[col].nunique()}, примеры: {df[col].unique()[:10]}")
        if 'sin' in col_str or col_str == 'o':
            print(f"  Столбец Sin/G: '{col}', мин={df[col].min()}, макс={df[col].max()}")
        if col_str in ['g', 'glob', 'r.sun', 'rsun', 'global']:
            print(f"  Столбец G (r.sun): '{col}', мин={df[col].min()}, макс={df[col].max()}")
        if col_str == 'r' or col_str == 'ratio':
            print(f"  Столбец R (коэффициент): '{col}', мин={df[col].min()}, макс={df[col].max()}")

    # 5. Проверяем столбец O (15-й, индекс 14)
    if df.shape[1] >= 15:
        col_o = df.columns[14]
        print(f"\n=== СТОЛБЕЦ O (индекс 14) = '{col_o}' ===")
        print(f"  Первые 10 значений: {df.iloc[:10, 14].tolist()}")

    # 6. Проверяем столбец R (17-й, индекс 17)
    if df.shape[1] >= 18:
        col_r = df.columns[17]
        print(f"\n=== СТОЛБЕЦ R (индекс 17) = '{col_r}' ===")
        print(f"  Первые 10 значений: {df.iloc[:10, 17].tolist()}")

    # 7. Ищем данные для точки 94
    for col in df.columns:
        if 'cat' in str(col).lower():
            pt94 = df[df[col] == 94]
            if len(pt94) > 0:
                print(f"\n=== ТОЧКА 94 (первые 5 записей) ===")
                print(pt94.head().to_string())
            break

    return df


# Запуск диагностики
if __name__ == "__main__":
    diagnose_model_sheet()