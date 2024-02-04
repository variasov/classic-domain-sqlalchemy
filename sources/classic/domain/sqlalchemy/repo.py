from typing import Sequence, Tuple

from classic.domain.core.criteria import Criteria, Xor, And, Or, Invert
from classic.domain.core import Repo, Root, translate_for

from sqlalchemy import select, Select, and_, or_, not_, delete, ColumnElement
from sqlalchemy.orm import Session


class SQLAlchemyRepo(Repo[Root]):
    session: Session

    def __init__(self, session: Session):
        self.session = session

    def save(self, *objects: Root) -> None:
        for obj in objects:
            obj.invariants.must_be_satisfied()

        self.session.add_all(objects)
        self.session.flush()

    def remove(self, *objects: Root) -> None:
        for obj in objects:
            self.session.delete(obj)

    def remove_by_id(self, *object_ids: object) -> None:
        query = delete(self.root).where(
            self.root.id.in_(object_ids)
        )
        self.session.execute(query)

    def get(self, object_id: object) -> Root | None:
        return self.session.get(self.root, object_id)

    def find(
        self, criteria: Criteria,
        order_by: str = None,
        limit: int = None,
        offset: int = None,
    ) -> Sequence[Root]:

        query = select(self.root)
        query, condition = self._criteria_to_query(criteria, query)
        query = query.where(condition)

        if order_by:
            query = query.order_by(order_by)

        if limit:
            query = query.limit(limit)

        if offset:
            query = query.offset(offset)

        return self.session.execute(query).scalars().all()

    def exists(self, criteria: Criteria) -> bool:

        query = select(self.root)
        query, condition = self._criteria_to_query(criteria, query)
        query = select(query.where(condition).exists())

        return self.session.execute(query).scalar()

    def _criteria_to_query(self, criteria: Criteria, query: Select) -> Tuple[Select, ColumnElement]:
        try:
            translator = self._translators[criteria.__class__]
        except KeyError:
            raise ValueError(f'Для критерия {criteria} не найден метод-переводчик в {self}')

        return translator(query, criteria)

    @translate_for(And)
    def __translate_and__(self, criteria: And, query: Select):
        conditions = []
        for criteria_ in criteria.nested_criteria:
            query, condition = self._criteria_to_query(criteria_, query)
            conditions.append(condition)

        return query, and_(*conditions)

    @translate_for(Or)
    def __translate_or__(self, criteria: Or, query: Select):
        conditions = []
        for criteria_ in criteria.nested_criteria:
            query, condition = self._criteria_to_query(criteria_, query)
            conditions.append(condition)

        return query, or_(*conditions)

    @translate_for(Xor)
    def __translate_xor__(self, query: Select, criteria: Xor):
        query, left = self._criteria_to_query(criteria.left, query)
        query, right = self._criteria_to_query(criteria.right, query)
        return query, or_(
            and_(left, not_(right)),
            and_(right, not_(left)),
        )

    @translate_for(Invert)
    def __translate_invert__(self, criteria: Invert, query: Select):
        query, condition = self._criteria_to_query(criteria.nested_criteria, query)
        return query, not_(condition)
