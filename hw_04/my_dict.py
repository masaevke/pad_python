class MyDict:
    def __init__(self, size=8):
        if size < 1:
            raise ValueError('size must be positive')
        self._buckets = [[] for _ in range(size)]
        #далее для хранения всех ключей в порядке вставки сразу при инициализации добавлю 
        # отдельный список _keys
        self._keys = []
        self._len = 0

    #добавлю вспомогательный метод для поиска ключа в корзине
    #который дальше будет использоваться в методах __setitem__, __getitem__, __delitem__ и get
    def _find(self, key):
        bucket = self._buckets[hash(key) % len(self._buckets)]
        for i, (stored, _) in enumerate(bucket):
            if stored == key:
                return bucket, i
        return bucket, -1

    # это метод для добавления или обновления элемента словаря
    # например d[26] = 'слива'	 
    def __setitem__(self, key, value):
        bucket, i = self._find(key)  # ищем ключ в корзине
        if i >= 0: # если ключ уже есть, обновляем значение
            bucket[i] = (bucket[i][0], value) #замена кортежа целиком
            return
        # если ключа нет, добавляем новый элемент
        bucket.append((key, value))
        self._keys.append(key)
        # и увеличиваем длину словаря
        self._len += 1
        # если заполненность корзин превышает 2/3, увеличиваем их количество
        if self._len * 3 > len(self._buckets) * 2:  
            old = self._buckets
            # увеличиваем количество корзин в два раза
            self._buckets = [[] for _ in range(len(old) * 2)]
            # перераспределяем элементы по новым корзинам  
            # с вычислением нового хеша каждого ключа
            for bucket in old:
                for key, value in bucket:
                    self._buckets[hash(key) % len(self._buckets)].append((key, value))

    def __getitem__(self, key):
        bucket, i = self._find(key)
        if i < 0:
            raise KeyError(key)
        return bucket[i][1]

    def __delitem__(self, key):
        # ищем ключ в корзине
        bucket, i = self._find(key)
        if i < 0:
            raise KeyError(key) # ошибка как у обычного словаря
        self._keys.remove(bucket[i][0])
        del bucket[i]
        #не забываем уменьшить длину словаря
        self._len -= 1

    #метод для проверки наличия ключа в словаре
    def __contains__(self, key):
        return self._find(key)[1] >= 0

    def __len__(self):
        return self._len

    def get(self, key, default=None):
        bucket, i = self._find(key)
        return default if i < 0 else bucket[i][1]

    def stats(self):
        return len(self._buckets), self._len, max(map(len, self._buckets))

    def __iter__(self):
        return iter(self._keys)

    # метод для получения итератора по ключам словаря
    def keys(self):
        return iter(self)

    # метод для получения итератора по значениям словаря
    def values(self):
        return (self[key] for key in self)

    # метод для получения итератора по парам (ключ, значение) словаря
    def items(self):
        return ((key, self[key]) for key in self)


    def __repr__(self):
        pairs = ', '.join(f'{key!r}: {value!r}' for key, value in self.items())
        return 'MyDict({' + pairs + '})' # строковое представление словаря


if __name__ == '__main__':
    d = MyDict()
    for i in range(1000):
        d[f'key{i}'] = i
    assert len(d) == 1000
    assert d['key42'] == 42
    assert 'key7' in d and 'nope' not in d
    d['key42'] = -1
    assert d['key42'] == -1 and len(d) == 1000
    del d['key42']
    assert 'key42' not in d and len(d) == 999
    assert d.get('nope') is None and d.get('nope', 0) == 0
    for bad in (lambda: d['nope'], lambda: d.__delitem__('nope')):
        try:
            bad()
            assert False, 'ожидали KeyError'
        except KeyError:
            pass
    buckets, size, longest = d.stats()
    assert size == 999 and size <= buckets * 2 / 3
    print(d.stats())


# 1. Почему при увеличении таблицы пары нельзя просто скопировать в корзины с теми же номерами?
# Потому что номер корзины зависит от хеша ключа и количества корзин, 
# и при изменении количества корзин хеши нужно пересчитывать.

# 2. Что показывает stats() для 1000 строковых ключей?
# Для 1000 строковых ключей stats() вернёт (2048, 1000, N), где 
# 2048 - количество корзин
# 1000 - количество элементов 
# N - длина самой длинной корзины

# 3. И что он покажет, если ключами будут объекты, у которых __hash__ всегда возвращает 42?
# В этом случае все элементы попадут в одну корзину, и stats() покажет 
# (M, N, N), где M - количество корзин, N - количество элементов (все в одной корзине).



# Проверьте экспериментом, замерьте время вставки 1000 таких ключей
from time import perf_counter


class IdentKey:
    def __init__(self, value):
        self.value = value

    def __hash__(self):
        return 42

    def __eq__(self, other):
        # проверим, что другой объект тоже является IdentKey   
        if not isinstance(other, IdentKey): 
            return NotImplemented
        # если тип тот же, то сравниваем значения
        return self.value == other.value

# эксперимент с IdentKey
keys = [IdentKey(i) for i in range(1000)]
d = MyDict()

start = perf_counter()
for i, key in enumerate(keys):
    d[key] = i
elapsed = perf_counter() - start

assert len(d) == 1000
print(d.stats())
print(f'Время вставки: {elapsed:.6f} с')

# эксперимент с обычными строковыми ключами
keys = [f'key{i}' for i in range(1000)]
d = MyDict()

start = perf_counter()
for i, key in enumerate(keys):
    d[key] = i
elapsed = perf_counter() - start

assert len(d) == 1000
print(d.stats())
print(f'Время вставки обычных ключей: {elapsed:.6f} с')
