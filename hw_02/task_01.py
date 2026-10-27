"""Трекер коллекции монет: list, tuple, dict и set"""


def add_coin(coins, coins_by_id, coin):
    """добавление монеты (id, страна, номинал, год) с уникальным id"""
    if type(coin) is not tuple or len(coin) != 4:
        print("Монета должна быть кортежем (id, страна, номинал, год)")
        return False

    coin_id, country, denomination, year = coin
    # Проверка точного типа исключает bool: True иначе равен ID 1.
    if type(coin_id) is not int or coin_id <= 0:
        print("ID должен быть положительным целым числом")
        return False
    if type(country) is not str or not country.strip():
        print("Страна должна быть непустой строкой")
        return False
    if type(denomination) is not str or not denomination.strip():
        print("Номинал должен быть непустой строкой")
        return False
    if type(year) is not int or year <= 0:
        print("Год должен быть положительным целым числом")
        return False

    if coin_id in coins_by_id:
        print(f"Монета с id {coin_id} уже есть в коллекции")
        return False

    # Оба контейнера ссылаются на неизменяемый кортеж из int и str.
    coins.append(coin)
    coins_by_id[coin_id] = coin
    return True


def remove_coin(coins, coins_by_id, coin_id):
    """удаление монеты из списка и словаря по id"""
    if type(coin_id) is not int or coin_id <= 0:
        print("ID должен быть положительным целым числом")
        return False
    if coin_id not in coins_by_id:
        return False

    coin = coins_by_id[coin_id]
    # удаление элемента без обхода списка
    coins.remove(coin)
    del coins_by_id[coin_id]
    return True


def show_coins(coins):
    for coin_id, country, denomination, year in coins:
        print(f"{coin_id}. {country}, {denomination}, {year} г.")


def main():
    coins = []
    coins_by_id = {}

    add_coin(coins, coins_by_id, (1, "Россия", "1 рубль", 1997))
    add_coin(coins, coins_by_id, (2, "Россия", "10 рублей", 2010))
    add_coin(coins, coins_by_id, (3, "СССР", "5 копеек", 1961))

    print("Коллекция монет:")
    show_coins(coins)

    print("\nПоиск монеты по id 2:")
    coin = coins_by_id.get(2)
    if coin is not None:
        print(coin)
    else:
        print("Монета не найдена.")

    # сбор стран, без повторов
    countries = set()
    for coin in coins:
        countries.add(coin[1])

    print("\nУникальные страны в коллекции:")
    for country in sorted(countries):
        print(country)

    remove_coin(coins, coins_by_id, 1)
    print("\nКоллекция после удаления монеты с id 1:")
    show_coins(coins)


if __name__ == "__main__":
    main()
