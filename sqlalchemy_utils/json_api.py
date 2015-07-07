"""
To test:

- One-to-one relationships
- One-to-many relationships
- Many-to-one relationships
- Deep relationships
- Many-to-many self-referencing
- Deep many-to-many self-referencing

Exceptions

- What if mapping doesn't contain alias for given model
- Unknown relationships?
- Unknown fields

Other

- Sane SQL aliases
"""
from functools import reduce

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB

from .relationships import path_to_relationships


def select_aggregate(session, agg_expr, relationships):
    """
    Return a subquery for fetching an aggregate value of given aggregate
    expression and given sequence of relationships.

    The returned aggregate query can be used when updating denormalized column
    value with query such as:

    UPDATE table SET column = {aggregate_query}
    WHERE {condition}

    :param agg_expr:
        an expression to be selected, for example sa.func.count('1')
    :param relationships:
        Sequence of relationships to be used for building the aggregate
        query.
    """
    if len(relationships) > 1:
        alias = sa.orm.aliased(relationships[0].mapper.class_)
        query = session.query(agg_expr).select_from(alias)

        for relationship in relationships[0:-2]:
            query = query.join(
                getattr(alias, relationship.key),
                aliased=True
            )
            alias = relationship.mapper.class_

        query = query.join(
            getattr(alias, relationships[-2].key),
        )
    else:
        alias = relationships[0].mapper.class_
        query = session.query(agg_expr).select_from(alias)

    last = relationships[-1]
    condition = last.primaryjoin
    if last.secondary is not None:
        query = query.join(
            last.secondary,
            last.secondaryjoin
        )
    return query.filter(condition)


def s(value):
    return sa.text("'{}'".format(value))


def has_foreign_key(column):
    return column in (fk.parent for fk in column.table.foreign_keys)


class JSONMapping(object):
    def __init__(self, mapping=None):
        self.mapping = mapping
        self.inversed = dict(
            (value, key) for key, value in self.mapping.items()
        ) if mapping else None

    def transform(self, query, fields=None, include=None):
        pass

    def build_attributes(self, entity, fields=None):
        entity_name = self.inversed[entity]

        def skip_field(key, column):
            return (
                column.primary_key
                or has_foreign_key(column)
                or (
                    fields and
                    entity_name in fields and
                    key not in fields[entity_name]
                )
            )

        return sum(
            (
                [s(key), column]
                for key, column in
                sa.inspect(entity).columns.items()
                if not skip_field(key, column)
            ),
            []
        )

    def build_id_and_type(self, model):
        model_alias = self.inversed[model]
        return [
            s('id'),
            model.id,
            s('type'),
            s(model_alias),
        ]

    def build_attrs_and_relationships(self, session, entity, fields):
        json_fields = []
        attrs = self.build_attributes(
            entity,
            fields=fields
        )
        json_relationships = self.build_relationships(
            session,
            entity,
            fields
        )

        if attrs:
            json_fields.append(s('attributes'))
            json_fields.append(sa.func.json_build_object(*attrs))

        if json_relationships:
            json_fields.append(s('relationships'))
            json_fields.append(
                sa.func.json_build_object(
                    *json_relationships
                )
            )
        return json_fields

    def build_relationships(self, session, entity, fields):
        model_alias = self.inversed[entity]
        relationships = [
            sa.inspect(entity).relationships[field]
            for field in fields[model_alias]
            if field in sa.inspect(entity).relationships.keys()
        ]

        json_relationships = []
        for relationship in relationships:
            json_relationships.append(
                s(relationship.key)
            )
            relationship_attrs = self.build_id_and_type(
                relationship.mapper.class_
            )
            func = sa.func.json_build_object(*relationship_attrs)

            if relationship.uselist:
                func = sa.func.array_agg(func)
            query = select_aggregate(session, func, [relationship]).correlate(
                entity
            )
            json_relationships.append(
                sa.func.json_build_object(
                    s('data'),
                    query.as_scalar()
                )
            )
        return json_relationships

    def build_data(self, session, entity, fields, include):
        model_alias = self.inversed[entity]
        json_fields = self.build_id_and_type(entity)
        if fields and model_alias in fields:
            json_fields.extend(self.build_attrs_and_relationships(
                session,
                entity,
                fields
            ))

        return [
            s('data'),
            sa.select(
                [
                    sa.func.array_agg(
                        sa.text('main_json.json_object')
                    )
                ],
                from_obj=sa.select(
                    [
                        sa.func.json_build_object(*json_fields)
                        .label('json_object')
                    ]
                ).alias('main_json')
            ).correlate(entity).as_scalar()
        ]

    def select(self, session, entity, fields=None, include=None):
        args = self.build_data(session, entity, fields, include)
        included = self.build_included(session, entity, fields, include)

        if included:
            args.extend(included)

        query = sa.select(
            [sa.func.json_build_object(*args)],
            from_obj=entity
        )

        print(query.compile(dialect=postgresql.dialect()))
        return query

    def build_included(self, session, entity, fields, include):
        included = []
        if include:
            included.append(s('included'))
            selects = []
            for path in include:
                relationships = path_to_relationships(path, entity)
                relationships = [r.property for r in relationships]

                for index, relationship in enumerate(relationships):
                    cls = relationship.mapper.class_
                    cls_alias = self.inversed[cls]
                    main_fields = self.build_id_and_type(cls)
                    if cls_alias in fields:
                        attrs = self.build_attributes(
                            cls,
                            fields=fields
                        )
                        main_fields.append(s('attributes'))
                        main_fields.append(sa.func.json_build_object(*attrs))

                    func = sa.cast(
                        sa.func.json_build_object(*main_fields),
                        JSONB
                    ).label('json_object')

                    selects.append(
                        select_aggregate(
                            session,
                            func,
                            list(reversed(relationships[0:index + 1]))
                        )
                        .correlate(entity)
                    )

            union = reduce(
                lambda a, b: a.union(b),
                selects
            )
            included.append(
                sa.select(
                    [sa.func.array_agg(sa.text('included.json_object'))],
                    from_obj=union.subquery('included')
                ).as_scalar()
            )
        return included


json = JSONMapping()


# WITH
#     (SELECT * FROM article) as articles,
#     (SELECT * FROM category WHERE id = articles.category_id) AS categories
# SELECT
#     json_build_object()
