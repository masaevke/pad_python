class LRUCache:
    def __init__(self, capacity: int):
        if capacity < 1:
            raise ValueError('capacity должно быть >= 1')
        self._capacity = capacity
        self._store: dict = {}  # key -> value

    def get(self, key, default=None):
        if key not in self._store:
            return default
        # перемещаем в конец - ключ становится самым свежим
        self._store[key] = self._store.pop(key)
        return self._store[key]

    def put(self, key, value):
        if key in self._store:
            del self._store[key]
            self._store[key] = value
        else:
            if len(self._store) >= self._capacity:
                # удаляем самый старый (первый) элемент
                first_key = next(iter(self._store))
                del self._store[first_key]
            self._store[key] = value

    def __len__(self):
        return len(self._store)

    def __contains__(self, key):
        return key in self._store


if __name__ == '__main__':
    c = LRUCache(2)
    c.put('a', 1)
    c.put('b', 2)
    assert c.get('a') == 1        # теперь 'a' свежее, чем 'b'
    c.put('c', 3)                 # вытесняется 'b'
    assert 'b' not in c and c.get('a') == 1 and c.get('c') == 3
    c.put('a', 10)                # перезапись делает 'a' свежим
    c.put('d', 4)                 # вытесняется 'c'
    assert c.get('c') is None and c.get('a') == 10 and len(c) == 2


    # проверка: 'in' не обновляет свежесть
    c2 = LRUCache(2)
    c2.put('x', 1)
    c2.put('y', 2)
    assert 'x' in c2               # проверка - не делает 'x' свежим
    c2.put('z', 3)                 # вытесняется 'x' (самый старый, не обновлялся)
    assert 'x' not in c2
    assert 'y' in c2 and 'z' in c2

    # граничный случай: capacity = 1
    c3 = LRUCache(1)
    c3.put(1, 10)
    assert c3.get(1) == 10
    c3.put(2, 20)
    assert c3.get(1) is None
    assert c3.get(2) == 20

    print('all tests passed')


# Для получения самого старого ключа использована конструкция
# next(iter(self._store)):
# dict сохраняет порядок вставки, а при обращении ключ переносится в конец.
# Затем удаляем его через del self._store[first_key] - в среднем за O(1).
# Однако получение первого ключа может просматривать пропуски после удалений,
# поэтому строгое O(1) для всей операции эта конструкция не гарантирует.
