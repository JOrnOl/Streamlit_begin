import pandas as pd
import numpy as np

# Реальные средние температуры (примерные данные) для городов по сезонам
seasonal_temperatures = {
    "New York": {"winter": 0, "spring": 10, "summer": 25, "autumn": 15},
    "London": {"winter": 5, "spring": 11, "summer": 18, "autumn": 12},
    "Paris": {"winter": 4, "spring": 12, "summer": 20, "autumn": 13},
    "Tokyo": {"winter": 6, "spring": 15, "summer": 27, "autumn": 18},
    "Moscow": {"winter": -10, "spring": 5, "summer": 18, "autumn": 8},
    "Sydney": {"winter": 12, "spring": 18, "summer": 25, "autumn": 20},
    "Berlin": {"winter": 0, "spring": 10, "summer": 20, "autumn": 11},
    "Beijing": {"winter": -2, "spring": 13, "summer": 27, "autumn": 16},
    "Rio de Janeiro": {"winter": 20, "spring": 25, "summer": 30, "autumn": 25},
    "Dubai": {"winter": 20, "spring": 30, "summer": 40, "autumn": 30},
    "Los Angeles": {"winter": 15, "spring": 18, "summer": 25, "autumn": 20},
    "Singapore": {"winter": 27, "spring": 28, "summer": 28, "autumn": 27},
    "Mumbai": {"winter": 25, "spring": 30, "summer": 35, "autumn": 30},
    "Cairo": {"winter": 15, "spring": 25, "summer": 35, "autumn": 25},
    "Mexico City": {"winter": 12, "spring": 18, "summer": 20, "autumn": 15},
}

# Сопоставление месяцев с сезонами
month_to_season = {12: "winter", 1: "winter", 2: "winter",
                   3: "spring", 4: "spring", 5: "spring",
                   6: "summer", 7: "summer", 8: "summer",
                   9: "autumn", 10: "autumn", 11: "autumn"}

# Генерация данных о температуре
def generate_realistic_temperature_data(cities, num_years=10):
    dates = pd.date_range(start="2010-01-01", periods=365 * num_years, freq="D")
    data = []

    for city in cities:
        for date in dates:
            season = month_to_season[date.month]
            mean_temp = seasonal_temperatures[city][season]
            # Добавляем случайное отклонение
            temperature = np.random.normal(loc=mean_temp, scale=5)
            data.append({"city": city, "timestamp": date, "temperature": temperature})

    df = pd.DataFrame(data)
    df['season'] = df['timestamp'].dt.month.map(lambda x: month_to_season[x])
    return df

# Генерация данных
data = generate_realistic_temperature_data(list(seasonal_temperatures.keys()))
data.to_csv('temperature_data.csv', index=False)

# -----------------------------------------------------------------------------------------------------------------------

# Исполнение функции без использования паралельности:

# Task3 Напишем функцию которая будет записывать True если температура отличается больше чем на 2 градуса и False в обратном случае
def is_anomaly(row):
  return abs(row['temperature'] - row['mean']) > 2

# ------------------------------------------------------------------------------------------------------------------------------------

def weather_analys(data):
  my_data = data.copy(deep = True)
  # Сначала преобразуем формат наших данных в column timestamp в datetime
  my_data['timestamp'] = pd.to_datetime(my_data['timestamp'])

  # Task1 С помощью метода rollig() создадим окно в 30 дней и посчитаем в нем скользящее среднее с помощью mean()
  print("Task 1:")
  print(" ")
  my_data['move_avg'] = my_data['temperature'].rolling(window=30).mean()
  print(my_data)
  print("+-------------------------------------------------+")

  # Task2 Сгруппируем значения по сезону в каждом городе и вычислим среднюю температуру и стандартное отклонение
  print("Task 2:")
  print(" ")
  season_stats = my_data.groupby(['city','season'])['temperature'].agg(['mean', 'std'])
  print(season_stats)
  print("+-------------------------------------------------+")

  season_stats = season_stats.reset_index()
  season_stats.columns = ['city', 'season', 'mean', 'std']

  # Task3 Присвоим каждому сезону среднюю температуру. С помощью merge сливаем эти данные из таблицы season_stats
  print("Task 3:")
  print(" ")
  my_data = my_data.merge(season_stats[['city', 'season', 'mean']], on=['city', 'season'], how='left')

  # Построим столбец is_anomaly в котором будем записывать True если температура отличается больше чем на 2 градуса и False в обратном случае
  my_data['is_anomaly'] = my_data.apply(is_anomaly, axis = 1)
  print(my_data)


weather_analys(data)

# ------------------------------------------------------------------------------------------------------------------------------

# Распараллелим процессы используя modin:

import modin.pandas as mpd
import modin.config as modin_cfg

modin_data = mpd.DataFrame(data)
modin_cfg.Engine.put("Ray")

# Task3: Определение аномалии (векторизированная операция)
def is_anomaly_parallel(data):
    return (abs(data['temperature'] - data['mean']) > 2)

def weather_analys_parallel(data):
    my_data = data.copy(deep=True)

    # Task1: Преобразуем timestamp в datetime
    my_data['timestamp'] = mpd.to_datetime(my_data['timestamp'])

    # Task1: Скользящее среднее (ускорено с modin)
    print("Task 1:\n")
    my_data['move_avg'] = my_data['temperature'].rolling(window=30).mean()
    print(my_data)
    print("+-------------------------------------------------+")

    # Task2: Среднее и стандартное отклонение по сезону и городу
    print("Task 2:\n")
    season_stats = my_data.groupby(['city', 'season'])['temperature'].agg(['mean', 'std']).reset_index()
    print(season_stats)
    print("+-------------------------------------------------+")

    # Task3: Слияние данных
    print("Task 3:\n")
    my_data = my_data.merge(season_stats[['city', 'season', 'mean']], on=['city', 'season'], how='left')

    # Task3: Вычисление аномалий
    my_data['is_anomaly'] = is_anomaly_parallel(my_data)

    print(my_data)

weather_analys_parallel(data)

# -------------------------------------------------------------------------------------------------------------------------

import time
# Напишем функцию обертку для сравнения времени выполнения функций с параллельностью и без

def get_time(func, data):
  start_time = time.time()
  func(data)
  end_time = time.time()
  return end_time - start_time

# Сравним время выполнения функции с паралелльностью и без
print("Время выполнения задачи без использования параллельности:", get_time(weather_analys,data))
print("Время выполнения задачи с использованием параллельности:", get_time(weather_analys_parallel,modin_data))

# --------------------------------------------------------------------------------------------------------------------------

# Использование OpenWeatherMap API

import requests

# Впишем ключ в программу и выберем город Berlin (можно выбрать любой город из data) 
api_key = "a54a3857126c9644f9fe018702aec49c"
city = "Berlin"

# В ссылке зададим город и ключ
url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

# Отправим запрос по ссылке 
response = requests.get(url)

# Получим данные в формате JSON и извлечем оттуда температуру
open_weather_data = response.json()
curr_temperature = open_weather_data['main']['temp']
print(f"Temperature in {city} is {curr_temperature}'C")

# Сравним с исторической температурой полученной из dataframe в задании
curr_season = 'spring'

# Вставим из функции код создания таблицы в которой отображается средняя температура в это время года в выбранном городе
season_stats = data.groupby(['city','season'])['temperature'].agg(['mean', 'std'])
season_stats = season_stats.reset_index()
season_stats.columns = ['city', 'season', 'mean', 'std']

# Подберем из таблицы среднее значение из таблицы season_stats 
temperature = season_stats[(season_stats['city'] == city) & (season_stats['season'] == curr_season)]['mean'].iloc[0]
print(f"Historic average temperature in {city} is {temperature}'C")

print('-----------------------------------------------------')
diff = abs(temperature - curr_temperature)
if diff < 2:
  print(f"Difference({diff}) is just normall")
else:
  print(f"Anomal difference({diff}) is detected")
