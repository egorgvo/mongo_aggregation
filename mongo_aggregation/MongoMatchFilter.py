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

    def and_(self, *args, **kwargs):
        """
        Добавляет фильтр используя опертор and. Позволяет избежать ситуации, с дублированием ключей.
        При необходимости создает ключ $and.
        :param value: Устанавливаемое значение
        :return: список условия $or
        """
        # Складываем все в args
        args = list(args)
        if kwargs:
            args.append(kwargs)

        for arg in args:
            for key in arg:
                if key in self:
                    break
            else:
                self.update(arg)
                continue
            self.setdefault('$and', [])
            self['$and'].append(arg)

    def and_nor(self, *args):
        """
        Добавляет в mongo-запрос ключ $nor.
        Во избежание перезаписи ключа $nor, ключи $nor записываются в список условий $and.
        :param args: Список $nor
        :return: список условия $or
        """
        self.and_or(*args, negative=True)

    def and_or(self, *args, negative=False):
        """
        Добавляет в mongo-запрос ключ $or.
        Во избежание перезаписи ключа $or, ключи $or записываются в список условий $and.
        :param args: Список $or
        :param negative: Флаг для использования $nor
        :return: список условия $or
        """
        key = '$nor' if negative else '$or'
        or_dict = MongoMatchFilter()
        or_dict[key] = args[0] if len(args) == 1 and isinstance(args[0], list) else list(args)
        if len(or_dict[key]) == 1 and not negative:
            or_dict = or_dict[key][0]
        self.and_(or_dict)
