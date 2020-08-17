# coding=utf-8


class MongoMatchFilter(dict):
    """
    Класс, представляющий собой словарь raw-запроса к mongo.
    Реализует запрет повторной установки ключа $or:
    Поскольку для запроса используется словарь типа dict, возможно неожидаемое переопределение ключа $or.
    """
    def __setitem__(self, key, value):
        """
        Устанваливает ключи в словарь запроса mongo.
        Если устанавливается больше одного ключа $or, вызывается исключение.
        Для корректного использования нескольких $or-условий одновременно, используйте метод and_or().
        :param key: ключ словаря
        :param value: значение словаря по ключу
        :return:
        """
        if key == '$or' and key in self:
            raise KeyError('Невозможно установить ключ $or однозначно, т.к. ключ уже существует. '
                           'При повторном назначении ключа возможно его переопределение. '
                           'Для корректной установки второго $or воспользуйтесь методом and_or().')

        super(MongoMatchFilter, self).__setitem__(key, value)

    def and_or(self, *args):
        """
        Добавляет в mongo-запрос ключ $or. Во избежание неожиданной перезаписи ключа $or, ключи $or записываются в список условий $and.
        :param value: Устанавливаемое значение
        :return: список условия $or
        """
        if '$and' not in self:
            self['$and'] = []
        or_dict = MongoMatchFilter()
        or_dict['$or'] = args[0] if len(args) == 1 and isinstance(args[0], list) else list(args)
        if len(or_dict['$or']) == 1:
            or_dict = or_dict['$or'][0]
        self['$and'].append(or_dict)
        return self['$and'][-1].get('$or', self['$and'][-1])
