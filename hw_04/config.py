class Config:
    def __init__(self, data: dict):
        self._data = data

    def __getattr__(self, name: str):
        # __getattr__ вызывается только когда name нет в __dict__/классах
        if name.startswith('_'):
            raise AttributeError(name)
        try:
            value = self._data[name]
        except KeyError:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute {name!r}"
            ) from None
        # оборачиваем вложенные словари в Config
        if isinstance(value, dict):
            return Config(value)
        return value

    def __setattr__(self, name: str, value):
        # присваивание собственных атрибутов (типа _data) идёт через object.__setattr__
        if name.startswith('_'):
            object.__setattr__(self, name, value)
            return
        # если self._data ещё не создан, сначала создаём его
        if not hasattr(self, '_data'):
            object.__setattr__(self, '_data', {})
        # присваиваем в корневой словарь
        self._data[name] = value

    def to_dict(self):
        # вернем исходные словарь без копирования
        return self._data

    def __dir__(self):
        return list(self._data.keys())


if __name__ == '__main__':
    raw = {'db': {'host': 'localhost', 'port': 5432}, 'debug': True}
    cfg = Config(raw)
    assert cfg.debug is True
    assert cfg.db.host == 'localhost' and cfg.db.port == 5432
    assert getattr(cfg, 'timeout', 30) == 30
    assert hasattr(cfg.db, 'user') is False
    cfg.db.port = 6543                    # запись меняет исходный словарь
    assert raw['db']['port'] == 6543
    cfg.timeout = 10
    assert raw['timeout'] == 10
    assert cfg.to_dict() is raw
    assert 'db' in dir(cfg)               # подсказка: метод __dir__


# 1. hasattr и getattr со значением по умолчанию обрабатывают AttributeError:
# hasattr возвращает False, а getattr - указанное значение по умолчанию.
# Если выбросить KeyError, ошибка выйдет наружу класса

# 2. Атрибут _data записывается через object.__setattr__, который обходит __setattr__. 
# При чтении существующий _data находится обычным способом, поэтому __getattr__ не вызывается. 
# Если внутреннего атрибута нет, проверка name.startswith('_') сразу вызывает AttributeError, 
# прерывая рекурсию.