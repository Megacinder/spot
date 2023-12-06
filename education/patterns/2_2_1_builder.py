import abc


class Builder(metaclass=abc.ABCMeta):
    def __init__(self):
        self.product = Product()

    @abc.abstractmethod
    def _nbki_request(self):
        pass

    @abc.abstractmethod
    def _job_call(self):
        pass

    @abc.abstractmethod
    def _property_check(self):
        pass


class Product:
    def __init__(self):
        self.result = [] # ['БКИ проверен', "На работу позвонили", "Недвижимость есть"]


class ConcreteBuilder(Builder):
    def _nbki_request(self):
        self.product.result.append('БКИ проверен')

    def _job_call(self):
        self.product.result.append('На работу позвонили')

    def _property_check(self):
        self.product.result.append('Недвижимость есть')


class Director:
    def construct(self, builder):
        builder._nbki_request()
        builder._job_call()
        builder._property_check()
        return builder.product.result
