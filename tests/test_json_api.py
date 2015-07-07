import pytest
import sqlalchemy as sa

from sqlalchemy_utils.json_api import JSONMapping

from tests import TestCase


class TestSimpleSelect(TestCase):
    dns = 'postgres://postgres@localhost/sqlalchemy_utils_test'

    def create_models(self):
        class User(self.Base):
            __tablename__ = 'user'
            id = sa.Column(sa.Integer, primary_key=True)
            name = sa.Column(sa.String(255))

        class Category(self.Base):
            __tablename__ = 'category'
            id = sa.Column(sa.Integer, primary_key=True)
            name = sa.Column(sa.String(255))
            created_at = sa.Column(sa.DateTime)
            parent_id = sa.Column(sa.Integer, sa.ForeignKey('category.id'))
            parent = sa.orm.relationship(
                'Category',
                backref='subcategories',
                remote_side=[id],
                order_by=id
            )

        class Article(self.Base):
            __tablename__ = 'article'
            id = sa.Column(sa.Integer, primary_key=True)
            name = sa.Column(sa.String(255), index=True)
            content = sa.Column(sa.String(255))

            category_id = sa.Column(sa.Integer, sa.ForeignKey(Category.id))
            category = sa.orm.relationship(Category, backref='articles')

            author_id = sa.Column(sa.Integer, sa.ForeignKey(User.id))
            author = sa.orm.relationship(
                User,
                primaryjoin=author_id == User.id,
                backref='authored_articles'
            )

            owner_id = sa.Column(sa.Integer, sa.ForeignKey(User.id))
            owner = sa.orm.relationship(
                User,
                primaryjoin=owner_id == User.id,
                backref='owned_articles'
            )

        class Comment(self.Base):
            __tablename__ = 'comment'
            id = sa.Column(sa.Integer, primary_key=True)
            content = sa.Column(sa.String(255), index=True)
            article_id = sa.Column(sa.Integer, sa.ForeignKey(Article.id))
            article = sa.orm.relationship(Article, backref='comments')

            author_id = sa.Column(sa.Integer, sa.ForeignKey(User.id))
            author = sa.orm.relationship(User, backref='comments')

        self.Article = Article
        self.Category = Category
        self.Comment = Comment
        self.User = User

        self.json = JSONMapping({
            'articles': Article,
            'categories': Category,
            'comments': Comment
        })

    def setup_method(self, method):
        TestCase.setup_method(self, method)

        user = self.User(name='User 1')
        user2 = self.User(name='User 2')
        article = self.Article(
            name='Some article',
            author=user,
            owner=user2,
            category=self.Category(
                name='Some category',
                subcategories=[
                    self.Category(name='Subcategory 1'),
                    self.Category(name='Subcategory 2'),
                ]
            ),
            comments=[
                self.Comment(
                    content='Some comment',
                    author=user
                )
            ]
        )
        self.session.add(article)
        self.session.commit()

    @pytest.mark.parametrize(
        ('fields', 'result'),
        (
            (
                None,
                {
                    'data': [{
                        'type': 'articles',
                        'id': 1
                    }]
                }
            ),
            (
                {'articles': ['name', 'content']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': 1,
                        'attributes': {
                            'name': 'Some article',
                            'content': None
                        }
                    }]
                }
            ),
            (
                {'articles': ['name']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': 1,
                        'attributes': {
                            'name': 'Some article'
                        }
                    }]
                }
            ),
            (
                {'articles': ['name', 'content', 'category']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': 1,
                        'attributes': {
                            'name': 'Some article',
                            'content': None
                        },
                        'relationships': {
                            'category': {
                                'data': {'type': 'categories', 'id': 1}
                            }
                        }
                    }]
                }
            ),
            (
                {'articles': ['name', 'content', 'comments']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': 1,
                        'attributes': {
                            'name': 'Some article',
                            'content': None
                        },
                        'relationships': {
                            'comments': {
                                'data': [{'type': 'comments', 'id': 1}]
                            }
                        }
                    }]
                }
            ),
            (
                {'articles': ['name', 'content', 'comments', 'category']},
                {
                    'data': [{
                        'type': 'articles',
                        'id': 1,
                        'attributes': {
                            'name': 'Some article',
                            'content': None
                        },
                        'relationships': {
                            'comments': {
                                'data': [{'type': 'comments', 'id': 1}]
                            },
                            'category': {
                                'data': {'type': 'categories', 'id': 1}
                            }
                        }
                    }]
                }
            )
        )
    )
    def test_fields_parameter(self, fields, result):
        query = self.json.select(self.session, self.Article, fields=fields)
        assert self.session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('fields', 'include', 'result'),
        (
            (
                {'articles': ['name', 'content', 'category']},
                ['category'],
                {
                    'data': [{
                        'type': 'articles',
                        'id': 1,
                        'attributes': {
                            'name': 'Some article',
                            'content': None
                        },
                        'relationships': {
                            'category': {
                                'data': {'type': 'categories', 'id': 1}
                            }
                        }
                    }],
                    'included': [{
                        'type': 'categories',
                        'id': 1
                    }]
                }
            ),
            (
                {'categories': ['name']},
                ['category'],
                {
                    'data': [{
                        'type': 'articles',
                        'id': 1,
                    }],
                    'included': [{
                        'type': 'categories',
                        'id': 1,
                        'attributes': {
                            'name': 'Some category'
                        }
                    }]
                }
            ),
            (
                {'articles': ['category'], 'categories': ['name']},
                ['category'],
                {
                    'data': [{
                        'type': 'articles',
                        'id': 1,
                        'relationships': {
                            'category': {
                                'data': {'type': 'categories', 'id': 1}
                            }
                        }
                    }],
                    'included': [{
                        'type': 'categories',
                        'id': 1,
                        'attributes': {
                            'name': 'Some category'
                        }
                    }]
                }
            ),
            (
                {
                    'articles': ['name', 'content', 'category'],
                    'categories': ['name']
                },
                ['category'],
                {
                    'data': [{
                        'type': 'articles',
                        'id': 1,
                        'attributes': {
                            'name': 'Some article',
                            'content': None
                        },
                        'relationships': {
                            'category': {
                                'data': {'type': 'categories', 'id': 1}
                            }
                        }
                    }],
                    'included': [{
                        'type': 'categories',
                        'id': 1,
                        'attributes': {
                            'name': 'Some category'
                        }
                    }]
                }
            ),
            (
                {
                    'articles': ['name', 'content', 'category', 'comments'],
                    'categories': ['name'],
                    'comments': ['content']
                },
                ['category', 'comments'],
                {
                    'data': [{
                        'type': 'articles',
                        'id': 1,
                        'attributes': {
                            'name': 'Some article',
                            'content': None
                        },
                        'relationships': {
                            'category': {
                                'data': {'type': 'categories', 'id': 1}
                            },
                            'comments': {
                                'data': [{'type': 'comments', 'id': 1}]
                            }
                        }
                    }],
                    'included': [
                        {
                            'type': 'categories',
                            'id': 1,
                            'attributes': {'name': 'Some category'}
                        },
                        {
                            'type': 'comments',
                            'id': 1,
                            'attributes': {'content': 'Some comment'}
                        },
                    ]
                }
            ),
        )
    )
    def test_include_parameter(self, fields, include, result):
        query = self.json.select(
            self.session,
            self.Article,
            fields=fields,
            include=include
        )
        assert self.session.execute(query).scalar() == result

    @pytest.mark.parametrize(
        ('fields', 'include', 'result'),
        (
            (
                {
                    'articles': ['name', 'content', 'category'],
                    'categories': ['name', 'subcategories'],
                },
                ['category.subcategories'],
                {
                    'data': [{
                        'type': 'articles',
                        'id': 1,
                        'attributes': {
                            'name': 'Some article',
                            'content': None
                        },
                        'relationships': {
                            'category': {
                                'data': {'type': 'categories', 'id': 1}
                            }
                        }
                    }],
                    'included': [
                        {
                            'type': 'categories',
                            'id': 1,
                            'attributes': {'name': 'Some category'},
                            'relationships': {
                                'subcategories': [
                                    {'data': {'type': 'categories', 'id': 2}},
                                    {'data': {'type': 'categories', 'id': 3}}
                                ]
                            }
                        },
                        {
                            'type': 'categories',
                            'id': 1,
                            'attributes': {'name': 'Some category'},
                            'relationships': {
                                'subcategories': None
                            }
                        },
                        {
                            'type': 'categories',
                            'id': 1,
                            'attributes': {'name': 'Some category'},
                            'relationships': {
                                'subcategories': None
                            }
                        },
                    ]
                }
            ),
        )
    )
    def test_deep_relationships(self, fields, include, result):
        query = self.json.select(
            self.session,
            self.Article,
            fields=fields,
            include=include
        )
        assert self.session.execute(query).scalar() == result
