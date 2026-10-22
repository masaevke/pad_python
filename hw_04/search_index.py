import re


class SearchIndex:
    """
    index хранит слова и множества документов, в которых они встречаются
    _documents хранит документы и словари с количеством слов в них
    _counts хранит слова и их общее количество во всех документах, 
    будет использоваться для ранжирования слов по частоте появления во всех документах.
    Например, для удаления не нужно перебирать весь индекс: 
    слова документа уже доступны через self._documents[doc_id]
    """
    def __init__(self):
        self.index = {} 
        self._documents = {} 
        self._counts = {} 

    def add(self, doc_id, text):
        """
        добавление или изменение документа в индексе
        """
        self.remove(doc_id) #удаляем старую версию документа, если она существует
        counts = {}
        for word in re.findall(r'\w+', text.lower()):
            counts[word] = counts.get(word, 0) + 1
        self._documents[doc_id] = counts
        for word, count in counts.items():
            self.index.setdefault(word, set()).add(doc_id)
            self._counts[word] = self._counts.get(word, 0) + count

    def search_all(self, *words):
        """
        поиск документов, которые содержат все указанные слова
        """
        query = set()
        for word in words:
            query.update(re.findall(r'\w+', word.lower()))

        if not query:
            return set()

        result = None
        for word in query:
            documents = self.index.get(word, set())
            if result is None:
                result = documents.copy()
            else:
                result = result & documents
            if not result:
                break
        return result

    def search_any(self, *words):
        """
        поиск документов, которые содержат хотя бы одно из указанных слов
        """
        result = set()
        for word in words:
            for token in re.findall(r'\w+', word.lower()):
                result = result | self.index.get(token, set())
        return result

    def search_ranked(self, query, limit=10):
        """
        сортирует документы по сумме вхождений уникальных слов запроса
        """
        if limit <= 0:
            return []
        words = set(re.findall(r'\w+', query.lower()))
        scores = {}
        for word in words:
            for doc_id in self.index.get(word, set()):
                count = self._documents[doc_id][word]
                scores[doc_id] = scores.get(doc_id, 0) + count
        ranked = sorted(scores, key=lambda doc_id: (-scores[doc_id], doc_id))
        return ranked[:limit]

    def remove(self, doc_id):
        """
        удаление документа из индекса по его айди
        """
        if doc_id not in self._documents:
            return

        for word, count in self._documents[doc_id].items():
            self.index[word].remove(doc_id)
            self._counts[word] -= count
            if not self.index[word]:
                del self.index[word]
                del self._counts[word]
        del self._documents[doc_id]

    def top_words(self, n):
        """
        возвращает список из n самых часто встречающихся слов во всех документах
        """
        if n <= 0:
            return []
        ranked = sorted(
            self._counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
        return ranked[:n]



if __name__ == '__main__':
    idx = SearchIndex()
    idx.add(1, 'Python is a programming language.')
    idx.add(2, 'A dict is a hash table; a set is a hash table too!')
    idx.add(3, 'Python dict and set are fast.')
    assert idx.search_all('python') == {1, 3}
    assert idx.search_all('dict', 'set') == {2, 3}
    assert idx.search_all('python', 'hash') == set()
    assert idx.search_any('language', 'fast') == {1, 3}
    assert idx.search_all('PYTHON') == {1, 3}
    assert idx.search_all('nothing') == set()
    assert idx.top_words(2) == [('a', 5), ('is', 3)]
    assert idx.search_ranked('PYTHON, dict!') == [3, 1, 2]
    assert idx.search_ranked('python dict', limit=2) == [3, 1]
    assert idx.search_ranked('python python dict') == [3, 1, 2]
    assert idx.search_ranked('a is') == [2, 1]
    assert idx.search_ranked('python') == [1, 3]
    assert idx.search_ranked('nothing') == []
    assert idx.search_ranked('') == []
    assert idx.search_ranked('!!!') == []
    assert idx.search_ranked('python', limit=0) == []
    assert idx.search_ranked('python', limit=-1) == []
    idx.remove(1)
    assert idx.search_all('python') == {3}
    assert idx.search_any('language', 'programming') == set()
    assert idx.search_ranked('python dict') == [3, 2]
    idx.add(3, 'Totally new text')
    assert idx.search_all('python') == set()
    assert idx.search_all('new') == {3}
    assert idx.search_ranked('python dict') == [2]
    assert idx.search_ranked('new') == [3]
    print(idx.index)
