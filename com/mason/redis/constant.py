#!/usr/bin/env python3
# -*- coding: utf-8 -*-


class Constant:
    class ConstError(TypeError):
        pass

    class ConstCaseError(ConstError):
        pass

    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise self.ConstError("Can't change const.%s" % name)
        if not name.isupper():
            raise self.ConstCaseError('Const name "%s" is not all uppercase' % name)
        self.__dict__[name] = value


const = Constant()

const.ONE_WEEK_IN_SECONDS = 7 * 24 * 60 * 60
# 将一天的秒数（86 400）除以文章展示一天所需的支持票数量（200）得出的
const.VOTE_SCORES = 432.0

const.ARTICLES_PER_PAGE = 25

const.SESSION_LIMIT = 1000000
