class Money:
    # выделяю только 2 артибута, чтобы нельзя было добавлять новые атрибуты динамически
    __slots__ = ("amount", "currency")

    def __init__(self, amount, currency):
        if type(amount) is not int:
            raise TypeError('amount должен быть целым числом')
        if not isinstance(currency, str):
            raise TypeError('currency должна быть строкой')
        # как в подсказке 
        object.__setattr__(self, "amount", amount)
        object.__setattr__(self, "currency", currency)

    # запрещаю добавление и удаление атрибутов после инициализации
    def __setattr__(self, name, value):
        raise AttributeError(f"{type(self).__name__} is immutable")

    # запрещаю удаление атрибутов после инициализации
    def __delattr__(self, name):
        raise AttributeError(f"{type(self).__name__} is immutable")

    # два объекта равны, если совпадают сумма и валюта; 
    # сравнение с объектом другого типа возвращает NotImplemented;
    def __eq__(self, other):
        if not isinstance(other, Money):
            return NotImplemented
        return self.amount == other.amount and self.currency == other.currency

    # проверка контракта a == b  =>  hash(a) == hash(b)
    def __hash__(self):
        return hash((self.amount, self.currency))

    def __add__(self, other):
        if not isinstance(other, Money):
            return NotImplemented
        if self.currency != other.currency:
            raise ValueError("Cannot add Money with different currencies")
        return Money(self.amount + other.amount, self.currency)

    # вернуть строковое представление объекта в виде Money(amount, currency)
    def __repr__(self):
        return f"Money({self.amount!r}, {self.currency!r})"

print(Money(1000, 'RUB'))
# print(Money('RUB', 'RUB'))



# проверка условия задачи 
# после создания поля изменить нельзя: m.amount = 5 и m.extra = 1 вызывают AttributeError
m = Money(100, 'RUB')

try:
    m.amount = 5
    assert False, 'Должен быть AttributeError при изменении amount'
except AttributeError:
    print('Изменение amount запрещено')

try:
    m.extra = 1
    assert False, 'Должен быть AttributeError при добавлении extra'
except AttributeError:
    print('Добавление extra запрещено')

assert m.amount == 100
assert not hasattr(m, 'extra')


# проверка m1 + m2 складывает суммы одной валюты и возвращает новый объект; 
# для разных валют бросает ValueError;

m1 = Money(100, 'RUB')
m2 = Money(200, 'RUB')
m3 = Money(300, 'USD')

assert m1 + m2 == Money(300, 'RUB') # если true, то сложение прошло корректно
try:
    m1 + m3
    assert False, 'Должен быть ValueError при сложении разных валют'
except ValueError:
    print('Сложение разных валют запрещено')

