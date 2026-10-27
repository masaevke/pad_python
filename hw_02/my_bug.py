# 1. Багованный код
countries = ("Россия", "СССР")
coins_by_country = dict.fromkeys(countries, [])
coins_by_country["Россия"].append(("1 рубль", 1997))
coins_by_country["СССР"].append(("5 копеек", 1961))
print("Багованная версия:")
print("Россия:", coins_by_country["Россия"])
print("СССР:", coins_by_country["СССР"])
print("Общий список:", coins_by_country["Россия"] is coins_by_country["СССР"])

# 2. Объяснение.
# Монеты обеих стран попадают в каждый раздел каталога
# dict.fromkeys(countries, []) не создаёт отдельный список для каждого ключа,
# а записывает во все значения ссылку на один переданный список
# Поэтому append через любой ключ изменяет общий объект, видимый через оба ключа

# 3. Исправленная версия: новый список создаётся отдельно для каждой страны
countries = ("Россия", "СССР")
coins_by_country = {}
for country in countries:
    coins_by_country[country] = []

coins_by_country["Россия"].append(("1 рубль", 1997))
coins_by_country["СССР"].append(("5 копеек", 1961))
print("\nИсправленная версия:")
print("Россия:", coins_by_country["Россия"])
print("СССР:", coins_by_country["СССР"])
#не совпадает с багованным
print("Общий список:", coins_by_country["Россия"] is coins_by_country["СССР"]) 
