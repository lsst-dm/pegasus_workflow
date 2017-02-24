import jinja2 as jinja


class Cowriter:
    """Creates Executor's configuration file for a given task.

    Parameters
    ----------
    name : `str`
        Name of the Jinja template to use, defaults to 'exec'
    path : `str`, optional
        Path to directory with Jinja templates, defaults to 'templates'.
    """

    # Counts task.
    counter = {}

    def __init__(self, name, path='templates'):
        self.env = jinja.Environment(loader=jinja.FileSystemLoader(path))
        self.tmpl = self.env.get(name + '.jinja')

    def write(self, name, args, in_path, out_path):
        """Writes Executor's configuration to a uniquely named file.

        Paramters
        ---------
        name : `str`
            Name of the task.
        args : `list` of `str`
            Task's command line arguments.
        in_path : `str`
            Root of the input dataset repository.
        out_path : `str`
            Root of the output dataset repository.
        """
        s = self.tmpl.render(name=name, args=args,
                             in_path=in_path, out_path=out_path)
        number = self.__class__.counter.setdefault(name, 0)
        filename = '%s-%d.json' % (name, number)
        self.__class__.counter[name] += 1
        with open(filename, 'w') as f:
            f.write(s)
