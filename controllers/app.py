class AppController:
    FACTORY = None
    CONNECTION = None

    @classmethod
    def set_factory(cls, factory):
        cls.FACTORY = factory

    @classmethod
    def set_connection(cls, connection):
        cls.CONNECTION = connection

    @classmethod
    def set_emailer(cls, emailer):
        cls.EMAILER = emailer

