from threading import Lock

class Singleton(type):
    _instances = {}
    _lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Model: # Override basic class behavior then seperate accross multiple childs [Inheretance and Overriding]
    def __iter__(self):
        """Iterate over public and private attributes of the instance."""
        for attr, value in self.__dict__.items():
            if not attr.startswith('_'):
                yield attr, value
            else:
                # Handle private attributes (e.g., single leading underscore)
                yield attr.lstrip('_'), value

    def __str__(self):
        """Return a formatted string representation of the instance for easy debugging."""
        public_attrs = [f"{attr}={value}" for attr, value in self.__dict__.items() if not attr.startswith('_')]
        private_attrs = [f"_{attr.lstrip('_')}={value}" for attr, value in self.__dict__.items() if
                         attr.startswith('_')]
        all_attrs = public_attrs + private_attrs
        return f"{type(self).__name__}(\n  " + ',\n  '.join(all_attrs) + "\n)"

    def __repr__(self):
        """Return an unambiguous representation of the instance, typically used for debugging."""
        attrs = ', '.join([f"{attr}={value!r}" for attr, value in self.__dict__.items()])
        return f"{type(self).__name__}({attrs})"

    def to_dict(self):
        """Return a dictionary representation of the instance."""
        return {attr.lstrip('_'): value for attr, value in self.__dict__.items()}


