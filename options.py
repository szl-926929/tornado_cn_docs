# coding=utf-8

#!/usr/bin/env python
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
命令行解析模块,让模块定义自己的选择
每个模块定义了将被添加到全局选项命名其自身的选择,例如.::

    from tornado.options import define, options

    define("mysql_host", default="127.0.0.1:3306", help="Main user DB")
    define("memcache_hosts", default="127.0.0.1:11011", multiple=True,
           help="Main user memcache servers")

    def connect():
        db = database.Connection(options.mysql_host)
        ...
你应用里的"main()"函数，并不需要知道所有的在整个程序中使用的选项，当模块加载时它们自动被加载
然而，所有的模块定义选项，必须在已导入命令行解析之前。
你的"main()"函数能够解析命令行或全局文件：

    tornado.options.parse_command_line()
    # or
    tornado.options.parse_config_file("/etc/server.conf")

命令行格式化是你所期望的 (``--myoption=myvalue``).
配置文件只是Python文件。全局名称成为选项，例如::
    myoption = "myvalue"
    myotheroption = "myothervalue"

支持 `datetimes <datetime.datetime>`, `timedeltas
<datetime.timedelta>`, ints, and floats (just pass a ``type`` kwarg to
`define`). 也接受多值选

`tornado.options.options` 是 `OptionParser`的单个实例, 在这个模块中的高级函数(`define`, `parse_command_line`, etc)
被简单调用.  你可能要创建额外的`OptionParser`实例去定义单独选项，诸如子命令(subcommands)

.. note::

   By default, several options are defined that will configure the
   standard `logging` module when `parse_command_line` or `parse_config_file`
   are called.  If you want Tornado to leave the logging configuration
   alone so you can manage it yourself, either pass ``--logging=none``
   on the command line or do the following to disable it in code::

       from tornado.options import options, parse_command_line
       options.logging = None
       parse_command_line()
"""

from __future__ import absolute_import, division, print_function, with_statement

import datetime
import numbers
import re
import sys
import os
import textwrap

from tornado.escape import _unicode
from tornado.log import define_logging_options
from tornado import stack_context
from tornado.util import basestring_type, exec_in


class Error(Exception): pass


class OptionParser(object):
    """一系列选项, object-like的字典
    通常情况下，通过静态函数的`tornado.options`模块访问，其中引用的全局实例。
    """
    def __init__(self):
        # we have to use self.__dict__ because we override setattr.
        self.__dict__['_options'] = {}              # 全局options
        self.__dict__['_parse_callbacks'] = []
        self.define("help", type=bool, help="show this help information",
                    callback=self._help_callback)

    def __getattr__(self, name):
        if isinstance(self._options.get(name), _Option):
            return self._options[name].value()
        raise AttributeError("Unrecognized option %r" % name)

    def __setattr__(self, name, value):
        if isinstance(self._options.get(name), _Option):
            return self._options[name].set(value)
        raise AttributeError("Unrecognized option %r" % name)

    def __iter__(self):
        return iter(self._options)

    def __getitem__(self, item):
        return self._options[item].value()

    def items(self):
        """A sequence of (name, value) pairs.

        .. versionadded:: 3.1
        """
        return [(name, opt.value()) for name, opt in self._options.items()]

    def groups(self):
        """The set of option-groups created by ``define``.

        .. versionadded:: 3.1
        """
        return set(opt.group_name for opt in self._options.values())

    def group_dict(self, group):
        """The names and values of options in a group.

        Useful for copying options into Application settings::

            from tornado.options import define, parse_command_line, options

            define('template_path', group='application')
            define('static_path', group='application')

            parse_command_line()

            application = Application(
                handlers, **options.group_dict('application'))

        .. versionadded:: 3.1
        """
        return dict(
            (name, opt.value()) for name, opt in self._options.items()
            if not group or group == opt.group_name)

    def as_dict(self):
        """所有的name:value选项
        .. versionadded:: 3.1
        """
        return dict(
            (name, opt.value()) for name, opt in self._options.items())

    def define(self, name, default=None, type=None, help=None, metavar=None,
               multiple=False, group=None, callback=None):
        """定义命令行选项
        如果`type``给定 (str, float, int, datetime, or timedelta)或’从default`推断,通过type来解析命令行参数. 如果 ``multiple`` is True
        接受逗号分隔(comma-separated values), option的值总是列表;对于多值整数，我们也接受`x:y`的语法转换成`range(x,y)`,十分有用的长整型ranges

        ``help`` and ``metavar`` 被用于构造自动生成的命令行帮助字符串。帮助消息被格式化像::

           --name=METAVAR      help string

        ``group`` 用于组的定义的选项中的逻辑组；默认情况下，命令行选项是由它们所定义的文件分组

        `name`: 命令行选项的名字必须全局唯一. 他们被命令行`parse_command_line`解析或从配置文件`parse_config_file`解析
        当`callback`给定，当选项改变时他将作为一个新值运行,这可以用来命令行结合
        和基于文件的选项::

            define("config", type=str, help="path to config file",
                   callback=lambda path: parse_config_file(path, final=False))
        """
        if name in self._options:
            raise Error("Option %r already defined in %s" % (name, self._options[name].file_name))

        frame = sys._getframe(0)                        #关于sys._getframe参考这里：http://share.beginman.cn:8001/blog/index.php/archives/165/
        options_file = frame.f_code.co_filename         # 返回当前文件名
        file_name = frame.f_back.f_code.co_filename
        if file_name == options_file:
            file_name = ""
        if type is None:
            if not multiple and default is not None:
                type = default.__class__
            else:
                type = str
        if group:
            group_name = group
        else:
            group_name = file_name

        #_Option实例
        self._options[name] = _Option(name, file_name=file_name,
                                      default=default, type=type, help=help,
                                      metavar=metavar, multiple=multiple,
                                      group_name=group_name,
                                      callback=callback)

    def parse_command_line(self, args=None, final=True):
        """Parses all options given on the command line (defaults to
        `sys.argv`).

        `args[0]`被忽略,因为它是在`sys.argv`中程序的名字
        返回未解析的选项所有参数列表

        args为定义的选项列表,第一个元素是空格，如 a.py --port=9000     由[' ', '--port=9000']组成
        如测试：

            define('port', default=8080, type=int, help='run this port', group='system')
            define('mysql',default='127.0.0.1:3306', type=str, help='use this mysql', group='system')

            print parse_command_line(['', '--port=9000'])        #返回 []
            print parse_command_line(['', '--notDefined=1000'])        #输入的选项没有在define定义内则出错：无法识别的命令行选项

        If ``final`` is ``False``, parse callbacks will not be run.
        This is useful for applications that wish to combine configurations
        from multiple sources.
        """
        if args is None:
            args = sys.argv     #返回文件名(全路径)

        remaining = []
        for i in range(1, len(args)):
            # All things after the last option are command line arguments
            if not args[i].startswith("-"):
                remaining = args[i:]
                break
            if args[i] == "--":
                remaining = args[i + 1:]
                break
            arg = args[i].lstrip("-")       #去除左边的'-'
            name, equals, value = arg.partition("=")        # 如--port=9000 ==> (port,=,9000)
            name = name.replace('-', '_')
            if not name in self._options:
                self.print_help()
                raise Error('Unrecognized command line option: %r' % name)
            option = self._options[name]
            if not equals:              #如果命令参数中没有'='
                if option.type == bool:
                    value = "true"
                else:
                    raise Error('Option %r requires a value' % name)
            option.parse(value)

        if final:
            self.run_parse_callbacks()

        return remaining

    def parse_config_file(self, path, final=True):
        """Parses and loads the Python config file at the given path.

        If ``final`` is ``False``, parse callbacks will not be run.
        This is useful for applications that wish to combine configurations
        from multiple sources.
        """
        config = {}
        with open(path) as f:
            exec_in(f.read(), config, config)
        for name in config:
            if name in self._options:
                self._options[name].set(config[name])

        if final:
            self.run_parse_callbacks()

    def print_help(self, file=None):
        """打印所有命令行选项标准错误（或其他文件）."""
        if file is None:
            file = sys.stderr
        print("Usage: %s [OPTIONS]" % sys.argv[0], file=file)
        print("\nOptions:\n", file=file)
        by_group = {}
        for option in self._options.values():
            by_group.setdefault(option.group_name, []).append(option)

        for filename, o in sorted(by_group.items()):
            if filename:
                print("\n%s options:\n" % os.path.normpath(filename), file=file)
            o.sort(key=lambda option: option.name)
            for option in o:
                prefix = option.name
                if option.metavar:
                    prefix += "=" + option.metavar
                description = option.help or ""
                if option.default is not None and option.default != '':
                    description += " (default %s)" % option.default
                lines = textwrap.wrap(description, 79 - 35)
                if len(prefix) > 30 or len(lines) == 0:
                    lines.insert(0, '')
                print("  --%-30s %s" % (prefix, lines[0]), file=file)
                for line in lines[1:]:
                    print("%-34s %s" % (' ', line), file=file)
        print(file=file)

    def _help_callback(self, value):
        if value:
            self.print_help()
            sys.exit(0)

    def add_parse_callback(self, callback):
        """Adds a parse callback, to be invoked when option parsing is done."""
        self._parse_callbacks.append(stack_context.wrap(callback))

    def run_parse_callbacks(self):
        for callback in self._parse_callbacks:
            callback()

    def mockable(self):
        """Returns a wrapper around self that is compatible with
        `mock.patch <unittest.mock.patch>`.

        The `mock.patch <unittest.mock.patch>` function (included in
        the standard library `unittest.mock` package since Python 3.3,
        or in the third-party ``mock`` package for older versions of
        Python) is incompatible with objects like ``options`` that
        override ``__getattr__`` and ``__setattr__``.  This function
        returns an object that can be used with `mock.patch.object
        <unittest.mock.patch.object>` to modify option values::

            with mock.patch.object(options.mockable(), 'name', value):
                assert options.name == value
        """
        return _Mockable(self)


class _Mockable(object):
    """`mock.patch` compatible wrapper for `OptionParser`.

    As of ``mock`` version 1.0.1, when an object uses ``__getattr__``
    hooks instead of ``__dict__``, ``patch.__exit__`` tries to delete
    the attribute it set instead of setting a new one (assuming that
    the object does not catpure ``__setattr__``, so the patch
    created a new attribute in ``__dict__``).

    _Mockable's getattr and setattr pass through to the underlying
    OptionParser, and delattr undoes the effect of a previous setattr.
    """
    def __init__(self, options):
        # Modify __dict__ directly to bypass __setattr__
        self.__dict__['_options'] = options
        self.__dict__['_originals'] = {}

    def __getattr__(self, name):
        return getattr(self._options, name)

    def __setattr__(self, name, value):
        assert name not in self._originals, "don't reuse mockable objects"
        self._originals[name] = getattr(self._options, name)
        setattr(self._options, name, value)

    def __delattr__(self, name):
        setattr(self._options, name, self._originals.pop(name))


class _Option(object):
    UNSET = object()

    def __init__(self, name, default=None, type=basestring_type, help=None,
                 metavar=None, multiple=False, file_name=None, group_name=None,
                 callback=None):
        if default is None and multiple:
            default = []
        self.name = name
        self.type = type
        self.help = help
        self.metavar = metavar
        self.multiple = multiple
        self.file_name = file_name
        self.group_name = group_name
        self.callback = callback
        self.default = default
        self._value = _Option.UNSET

    def value(self):
        return self.default if self._value is _Option.UNSET else self._value

    def parse(self, value):
        _parse = {
            datetime.datetime: self._parse_datetime,
            datetime.timedelta: self._parse_timedelta,
            bool: self._parse_bool,
            basestring_type: self._parse_string,
        }.get(self.type, self.type)
        if self.multiple:
            self._value = []
            for part in value.split(","):
                if issubclass(self.type, numbers.Integral):
                    # allow ranges of the form X:Y (inclusive at both ends)
                    lo, _, hi = part.partition(":")
                    lo = _parse(lo)
                    hi = _parse(hi) if hi else lo
                    self._value.extend(range(lo, hi + 1))
                else:
                    self._value.append(_parse(part))
        else:
            self._value = _parse(value)
        if self.callback is not None:
            self.callback(self._value)
        return self.value()

    def set(self, value):
        if self.multiple:
            if not isinstance(value, list):
                raise Error("Option %r is required to be a list of %s" %
                            (self.name, self.type.__name__))
            for item in value:
                if item is not None and not isinstance(item, self.type):
                    raise Error("Option %r is required to be a list of %s" %
                                (self.name, self.type.__name__))
        else:
            if value is not None and not isinstance(value, self.type):
                raise Error("Option %r is required to be a %s (%s given)" %
                            (self.name, self.type.__name__, type(value)))
        self._value = value
        if self.callback is not None:
            self.callback(self._value)

    # Supported date/time formats in our options
    _DATETIME_FORMATS = [
        "%a %b %d %H:%M:%S %Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
        "%Y%m%d %H:%M:%S",
        "%Y%m%d %H:%M",
        "%Y-%m-%d",
        "%Y%m%d",
        "%H:%M:%S",
        "%H:%M",
    ]

    def _parse_datetime(self, value):
        for format in self._DATETIME_FORMATS:
            try:
                return datetime.datetime.strptime(value, format)
            except ValueError:
                pass
        raise Error('Unrecognized date/time format: %r' % value)

    _TIMEDELTA_ABBREVS = [
        ('hours', ['h']),
        ('minutes', ['m', 'min']),
        ('seconds', ['s', 'sec']),
        ('milliseconds', ['ms']),
        ('microseconds', ['us']),
        ('days', ['d']),
        ('weeks', ['w']),
    ]

    _TIMEDELTA_ABBREV_DICT = dict(
        (abbrev, full) for full, abbrevs in _TIMEDELTA_ABBREVS
        for abbrev in abbrevs)

    _FLOAT_PATTERN = r'[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?'

    _TIMEDELTA_PATTERN = re.compile(
        r'\s*(%s)\s*(\w*)\s*' % _FLOAT_PATTERN, re.IGNORECASE)

    def _parse_timedelta(self, value):
        try:
            sum = datetime.timedelta()
            start = 0
            while start < len(value):
                m = self._TIMEDELTA_PATTERN.match(value, start)
                if not m:
                    raise Exception()
                num = float(m.group(1))
                units = m.group(2) or 'seconds'
                units = self._TIMEDELTA_ABBREV_DICT.get(units, units)
                sum += datetime.timedelta(**{units: num})
                start = m.end()
            return sum
        except Exception:
            raise

    def _parse_bool(self, value):
        return value.lower() not in ("false", "0", "f")

    def _parse_string(self, value):
        return _unicode(value)


options = OptionParser()
"""Global options object.

All defined options are available as attributes on this object.
"""


def define(name, default=None, type=None, help=None, metavar=None,
           multiple=False, group=None, callback=None):
    """在全局空间中定义选项
    See `OptionParser.define`.
    """
    return options.define(name, default=default, type=type, help=help,
                          metavar=metavar, multiple=multiple, group=group,
                          callback=callback)


def parse_command_line(args=None, final=True):
    """解析命令行全局选项.

    See `OptionParser.parse_command_line`.
    """
    return options.parse_command_line(args, final=final)


def parse_config_file(path, final=True):
    """Parses global options from a config file.

    See `OptionParser.parse_config_file`.
    """
    return options.parse_config_file(path, final=final)


def print_help(file=None):
    """Prints all the command line options to stderr (or another file).

    See `OptionParser.print_help`.
    """
    return options.print_help(file)


def add_parse_callback(callback):
    """Adds a parse callback, to be invoked when option parsing is done.

    See `OptionParser.add_parse_callback`
    """
    options.add_parse_callback(callback)


# Default options
define_logging_options(options)
