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




##---------------------------------------------------##
def total_by_currency(payments):
    totals = {}
    for payment in payments:
        if payment.currency in totals:
            totals[payment.currency] = totals[payment.currency] + payment
        else:
            totals[payment.currency] = payment
    return totals

payments = [
    Money(100, 'RUB'),
    Money(5, 'USD'),
    Money(250, 'RUB'),
]

# проверка функции total_by_currency, если сложилось
# несколько платежей одной и той же валюты
# то программа продолжится
assert total_by_currency(payments) == {
    'RUB': Money(350, 'RUB'),
    'USD': Money(5, 'USD'),
}
assert total_by_currency([]) == {}





##---------------------------------------------------##
# првоверки из задания
if __name__ == '__main__':
    assert len({Money(100, 'RUB'), Money(100, 'RUB'), Money(100, 'USD')}) == 2
    prices = {Money(100, 'RUB'): 'сто рублей'}
    assert prices[Money(100, 'RUB')] == 'сто рублей'
    assert Money(100, 'RUB') + Money(50, 'RUB') == Money(150, 'RUB')
    for action in (lambda m: setattr(m, 'amount', 5), lambda m: setattr(m, 'extra', 1)):
        try:
            action(Money(1, 'RUB'))
            assert False, 'ожидали AttributeError'
        except AttributeError:
            pass
    try:
        Money(1, 'RUB') + Money(1, 'USD')
        assert False, 'ожидали ValueError'
    except ValueError:
        pass
    pays = [Money(100, 'RUB'), Money(5, 'USD'), Money(250, 'RUB')]
    assert total_by_currency(pays) == {'RUB': Money(350, 'RUB'), 'USD': Money(5, 'USD')}


##---------------------------------------------------##
# что сломалось бы, если бы поля можно было менять после того, 
# как объект положили в множество или использовали как ключ.

# Например, если бы мы изменили amount у объекта, который уже находится в множестве,
# то хэш объекта изменился бы, и множество больше не смогло бы корректно его идентифицировать.
# Аналогично, если объект используется как ключ в словаре, доступ к значению 
# по этому ключу мог бы стать невозможным.


# В лекции это показано на классе Point: его хеш вычисляется по координатам x и y.
# После p = Point(1, 2) и visited = {p} изменение p.x = 100 меняет хеш объекта.
# Проверка p in visited может вернуть False, хотя объект остался в множестве:
# поиск использует новый хеш, а объект хранится по старому.
# При этом p in list(visited) вернёт True, поскольку список ищет перебором без хеша.
