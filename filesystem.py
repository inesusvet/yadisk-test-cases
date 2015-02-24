# -*- coding: utf-8 -*-
__author__ = 'Ivan Styazhkin <nesusvet@yandex-team.ru>'

import logging
import sys
from operator import itemgetter

try:
    from pymongo import Connection
    from pymongo.errors import ConnectionFailure
except ImportError:
    print 'Please run this script inside a python virtualenv. You can create new one locally with "bash setup_env.sh"'

ROOT = ''
SEPARATOR = '/'
FILE = 'F'
DIR = DIRECTORY = 'D'

logging.basicConfig(
    level='INFO',
)
logger = logging.getLogger('filesystem')


def strip_path(path):
    """
    Убираем последний слэш в пути

    >>> strip_path('/etc/nginx/')
    '/etc/nginx'
    >>> strip_path('/bla')
    '/bla'
    """
    return path.rstrip(SEPARATOR)


def build_path(path, name):
    """
    Строим путь до дочернего элемента

    >>> build_path('foo/bar', 'baz')
    'foo/bar/baz'
    >>> build_path('', 'foo')
    '/foo'
    """
    return '%s%s%s' % (path, SEPARATOR, name)


def split_path(path):
    """
    Разбиваем путь на две части -- путь до родительского элемента и имя дочернего элемента

    >>> split_path('/etc/nginx/nginx.conf')
    ['/etc/nginx', 'nginx.conf']
    >>> split_path('/')
    ['', '']
    """
    return path.rsplit(SEPARATOR, 1)


class BaseFileSystemError(Exception):
    """Базовая ошибка файловой системы"""


class FileNotFound(BaseFileSystemError):
    """Не нашли указанный файл"""


class ParentNotFound(BaseFileSystemError):
    """Нельзя создавать файлы без родителей"""


class NodeExistsAlready(BaseFileSystemError):
    """Файл или папка с указанным именем уже существует"""


class InvalidName(BaseFileSystemError):
    """Имя файла или папки не должно содержать слэш"""


class Node(dict):
    """
    Класс для представления информации об объекте файловой системы
    Обязательно имя объекта
    Тип объекта указаывает, файл это или папка
    Путь до объекта должен быть уникален
    Подразумевается один родителский объект
    """

    node_type = None
    key_list = ['type', 'parent_id', 'name', 'path']

    def __init__(self, name, **kwargs):
        logger.debug('Init node with name %r and %s', name, kwargs)
        kwargs['type'] = self.node_type
        parent = kwargs.pop('parent', None)

        if isinstance(parent, DirNode):
            kwargs['parent_id'] = parent['_id']

        if 'path' not in kwargs:
            parent_path = parent['path'] if isinstance(parent, DirNode) else ROOT
            kwargs['path'] = build_path(parent_path, name)

        logger.debug('Modified kwargs = %s', kwargs)
        super(Node, self).__init__([('name', name)], **kwargs)

    @classmethod
    def from_mongo(cls, kwargs):
        name = kwargs.pop('name')
        node_type = kwargs.pop('type', FILE)

        if node_type == FILE:
            return FileNode(name, **kwargs)

        elif node_type == DIR:
            return DirNode(name, **kwargs)

    def to_dict(self):
        dictionary = dict(
            (key_name, self[key_name])
            for key_name in self.key_list
            if self[key_name] is not None
        )
        if '_id' in self:
            dictionary['_id'] = self['_id']
        return dictionary


class FileNode(Node):
    node_type = FILE


class DirNode(Node):
    node_type = DIR


class MongoDBFilesystem(object):

    def __init__(self, mongo, database_name='local', collection_name='fs'):
        self.connection = mongo
        self.collection_name = collection_name
        self.db = getattr(self.connection, database_name)

    @property
    def fs(self):
        return getattr(self.db, self.collection_name)

    def save(self, node):
        logger.debug('Saving node %r to %r', node['name'], node['path'])
        return self.fs.save(node.to_dict())

    def get_children(self, node):
        logger.debug('Getting children of %r', node['path'])
        object_list = self.fs.find(dict(parent_id=node['_id']))
        return map(
            Node.from_mongo,
            object_list,
        )

    def get_parent(self, path):
        logger.debug('Getting parent of %r', path)
        path = strip_path(path)
        parent_path, _ = split_path(path)
        node = self.by_path(parent_path)
        return Node.from_mongo(node)

    def by_id(self, node_id):
        node = self.fs.find_one(dict(_id=node_id))
        if node is None:
            return

        return Node.from_mongo(node)

    def by_path(self, path):
        """Ожидаем, что путь уникален для каждого файла"""
        logger.debug('Getting node by path %r', path)
        path = strip_path(path)
        node = self.fs.find_one(dict(path=path))
        if node is None:
            return 

        return Node.from_mongo(node)


class Client(object):
    """
    Вы с коллегами решили написать свой Яндекс.Диск.
    В качестве первоочередной задачи вы выбрали создание файловой подсистемы.
    У файла есть содержимое и метаданные. Задачей хранения содержимого занялись ваши коллеги,
    а вы взяли на себя организацию хранения метаданных.

    Вам следует написать прототип файловой подсистемы, который работает с метаданными и умеет:
      * создавать папку;
      * создавать файл;
      * получать информацию о папке или файле;
      * получать листинг папки.

    Метаданные можно хранить в любой современной БД.
    """

    def __init__(self):
        self.fs = None

    def connect(self, **config):
        mongo = Connection(**config)
        self.fs = MongoDBFilesystem(mongo)

    def init(self):
        """Создаем корневой элемент файловой системы, если его нет"""
        root = self.fs.by_path(ROOT)
        if root is None:
            self.fs.fs.insert(dict(type=DIR, name='', path='', parent_id=None))

    def create_node(self, path, new_name, node_class):
        logger.debug('Creating node type %s in %r with name %r', node_class.node_type, path, new_name)

        if SEPARATOR in new_name:
            raise InvalidName('Invalid node name %r' % new_name)

        parent_path = strip_path(path)
        parent = self.fs.by_path(parent_path)
        if parent is None:
            raise ParentNotFound('There is no node %s' % parent_path)

        children = self.fs.get_children(parent)
        if new_name in children:
            raise NodeExistsAlready('There is file with name %s at path %s' % (new_name, parent_path))

        node = node_class(
            name=new_name,
            parent=parent,
        )
        self.fs.save(node)

    def create_directory(self, path, new_name):
        """Создаем запись о новой папке по указанному пути"""
        self.create_node(path, new_name, DirNode)

    def create_file(self, path, new_name):
        """Создаем запись о новом файле по указанному пути"""
        self.create_node(path, new_name, FileNode)

    def get_info(self, path):
        """Получаем всю известную информацию об объекте - имя, размер, путь, семейное положение"""
        path = strip_path(path)
        node = self.fs.by_path(path)
        if node is None:
            raise FileNotFound('There is no file %s' % path)
        return node.to_dict()

    def list_directory(self, path):
        """Возвращаем список имен файлов и папок, находящихся в указанной папке"""
        node = self.fs.by_path(path)
        children = self.fs.get_children(node)
        return map(
            itemgetter('name'),
            children,
        )

USAGE = """Meta information client for file system.

Usage: python filesystem.py <command> <path> [<args>]
Known commands:
  * init - creates root element if required (no path argument required)
  * list_dir - shows a directory's contents
  * new_file - creates a new file. Requires path and new file name(s)
  * new_dir - creates a new directory. Requires path and new directory name(s)
  * info - shows node's info
"""


def format_info(node):
    attrs = ['_id', 'type', 'path', 'name']
    return '\t'.join(
        map(
            lambda attr_name: '%s=%s' % (attr_name, node.get(attr_name)),
            attrs,
        )
    )


def main(command, path, args=None):
    """
    Подключаемся к БД и выполняем указанную команду.
    Сообщаем об ошибках цивилизованно
    """

    try:
        client = Client()
        client.connect()

        if command == 'new_file':
            for name in args:
                client.create_file(path, name)

        elif command == 'new_dir':
            for name in args:
                client.create_directory(path, name)

        elif command == 'list_dir':
            print 'Listing of %s' % path
            for name in client.list_directory(path):
                print name

        elif command == 'info':
            node_info = client.get_info(path)
            print format_info(node_info)

        elif command == 'init':
            print 'Creating root'
            client.init()

        else:
            logger.error('Unknown command %r', command)
            print USAGE

    except ConnectionFailure, ex:
        print 'ERROR: Connect to local Mongodb failed %s' % ex

    except BaseFileSystemError, ex:
        print 'ERROR: File system error %s' % ex


if __name__ == '__main__':
    # Когда будет больше команд и параметров - можно перейти на argparse
    if len(sys.argv) < 2:
        print USAGE
        sys.exit(0)

    if len(sys.argv) == 2:
        main(sys.argv[1], None)
        sys.exit(0)

    main(sys.argv[1], sys.argv[2], args=sys.argv[3:])
