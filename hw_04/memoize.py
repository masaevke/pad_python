from time import perf_counter


def memoize(func):
    cache = {} #кеш-словарь

    def cached_func(*args, **kwargs):
        named_args = tuple(sorted(kwargs.items()))
        key = (args, named_args) #формирование ключа
        try:
            hash(key) #проверка, можно ли использовать ключ в кеше
        except TypeError:
            return func(*args, **kwargs)

        if key in cache:
            cached_func.hits += 1 #увеличиваем счетчик попаданий в кеш
            return cache[key]

        cached_func.misses += 1 #увеличиваем счетчик промахов в кеше
        result = func(*args, **kwargs)
        cache[key] = result
        return result

    def cache_clear():
        """
        очистка и сброс счетчиков попаданий и промахов
        """
        cache.clear()
        cached_func.hits = 0
        cached_func.misses = 0

    cached_func.hits = 0
    cached_func.misses = 0
    cached_func.cache_clear = cache_clear
    #у возвращаемой функции сохраняются __name__ и __doc__ исходной функции
    cached_func.__name__ = func.__name__
    cached_func.__doc__ = func.__doc__
    return cached_func


if __name__ == '__main__':
    calls = []

    def slow_square(x, power=2):
        '''Возводит в степень.'''
        calls.append(x)
        return x ** power

    sq = memoize(slow_square)
    assert sq(3) == 9 and sq(3) == 9 and sq(3, power=3) == 27
    assert calls == [3, 3]
    assert (sq.hits, sq.misses) == (1, 2)
    assert sq.__name__ == 'slow_square' and sq.__doc__ == 'Возводит в степень.'
    total = memoize(sum)
    assert total([1, 2, 3]) == 6 and total([1, 2, 3]) == 6   # нехешируемый аргумент: вызов без кэша
    assert (total.hits, total.misses) == (0, 0)
    sq.cache_clear()
    assert (sq.hits, sq.misses) == (0, 0)
    sq(3)
    assert calls[-1] == 3




##-------------------------------------##
#Эксперимент с рекурсией
def fib(n):
    if n <= 1:
        return n
    result = fib(n - 1) + fib(n - 2)
    return result


#далее написал все с принтами, чтобы понять как работет
if __name__ == '__main__':
    #вариант А
    fast_fib = memoize(fib)
    print('создание fast_fib с пустым кэшем')
    print(f'hits={fast_fib.hits}, misses={fast_fib.misses}')

    print('\n1. fast_fib(5): результата нет, вызываем исходную fib(5)')
    result = fast_fib(5)
    assert result == 5
    assert (fast_fib.hits, fast_fib.misses) == (0, 1)
    print(f'Результат: {result}. В кэше сохранён результат для аргумента 5')
    print(f'hits={fast_fib.hits}, misses={fast_fib.misses}')

    print('\n2. fast_fib(6): результата для 6 нет, снова вызываем исходную fib')
    print('Внутри неё вызывается fib(5), а не fast_fib(5): кэш пропускается, так как это другая функция')
    result = fast_fib(6)
    assert result == 8
    assert (fast_fib.hits, fast_fib.misses) == (0, 2)
    print(f'Результат: {result}. В кэше теперь результаты для аргументов 5 и 6')
    print(f'hits={fast_fib.hits}, misses={fast_fib.misses}')

    print('\n3. Снова fast_fib(5): результат уже есть в кэше')
    result = fast_fib(5)
    assert result == 5
    assert (fast_fib.hits, fast_fib.misses) == (1, 2) 
    print('один раз попали в кэш, два раза промахнулись')
    print(f'Результат: {result}. Исходная fib не вызывается, так как результат уже есть в кэше')
    print(f'hits={fast_fib.hits}, misses={fast_fib.misses}')


    fast_fib.cache_clear()
    start = perf_counter()
    result_a = fast_fib(30)
    time_a = perf_counter() - start
    assert result_a == 832040
    assert fast_fib.misses == 1
    print(f'\nВариант А: результат={result_a}, misses={fast_fib.misses}, время={time_a:.6f} с')

    #далее вариант Б
    fib = memoize(fib)
    print('Имя fib теперь указывает на функцию с новым пустым кэшем')
    print(f'hits={fib.hits}, misses={fib.misses}')

    print('\n1. fib(5): рекурсивные вызовы тоже проходят через кэш')
    result = fib(5)
    assert result == 5
    assert (fib.hits, fib.misses) == (3, 6)
    print(f'Результат: {result}. В кэше результаты для аргументов от 0 до 5')
    print('6 промахов: каждое из значений 0, 1, 2, 3, 4, 5 вычислено один раз')
    print('3 попадания: повторные вызовы fib(1), fib(2), fib(3) взяты из кэша')
    print(f'hits={fib.hits}, misses={fib.misses}')

    print('\n2. fib(6): вычисляем только новый результат для 6')
    result = fib(6)
    assert result == 8
    assert (fib.hits, fib.misses) == (5, 7)
    print('Внутренние вызовы fib(5) и fib(4) находят готовые результаты')
    print(f'Результат: {result}. В кэше теперь результаты для аргументов от 0 до 6')
    print(f'hits={fib.hits}, misses={fib.misses}')

    print('\n3. Снова fib(5): результат уже есть в кэше')
    result = fib(5)
    assert result == 5
    assert (fib.hits, fib.misses) == (6, 7)
    print('Ещё одно попадание, новых вычислений нет')
    print(f'Результат: {result}, hits={fib.hits}, misses={fib.misses}')

    fib.cache_clear()
    start = perf_counter()
    result_b = fib(30)
    time_b = perf_counter() - start
    assert result_b == result_a
    assert fib.misses == 31
    print(f'\nВариант Б: результат={result_b}, misses={fib.misses}, время={time_b:.6f} с')
    if time_b > 0:
        print(f'Вариант Б быстрее варианта А в {time_a / time_b:.1f} раза')


# По правилу LEGB имя fib внутри исходной функции находится в глобальной области в момент вызова. 
# Присваивание fib = memoize(fib) меняет то, какую функцию находят рекурсивные вызовы (находят cached_func).
# В варианте А имя fib указывает на исходную функцию,
# поэтому рекурсия обходит кэш fast_fib и повторяет промежуточные вычисления.
# В варианте Б присваивание fib = memoize(fib) связывает имя fib с cached_func:
# рекурсивные вызовы проходят через кэш, и каждое значение вычисляется один раз