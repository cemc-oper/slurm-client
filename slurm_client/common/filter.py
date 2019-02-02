# coding: utf-8


class Filter(object):
    USE_IN_DEFAULT = False

    def __init__(self, name):
        self.name = name

    def apply(self, job_items):
        raise NotImplementedError
