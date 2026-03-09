import pandas as pd
import matplotlib.pyplot as plt

# Загрузите данные из вашего Excel файла
df = pd.read_excel('check.xlsx')

# Предположим, что у вас есть столбцы 'X', 'Y1' и 'Y2'
x = df['TIME']
y1 = df['MY_DATA']
y2 = df['TRUE_DATA']

# Создайте фигуру и сетку осей
fig, ax1 = plt.subplots()

# Постройте первый график на первой оси
ax1.plot(x, y1, color='blue', label='График 1')
ax1.set_xlabel('Ось X')
ax1.set_ylabel('Ось Y1', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')

# Создайте вторую ось, которая использует ту же ось X
ax2 = ax1.twinx()

# Постройте второй график на второй оси
ax2.plot(x, y2, color='red', label='График 2')
ax2.set_ylabel('Ось Y2', color='red')
ax2.tick_params(axis='y', labelcolor='red')

# Добавьте легенды
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

# Добавьте заголовок
plt.title('Два графика в одном')

# Сохраните или отобразите график
plt.show()
# plt.savefig('два_графика.png')