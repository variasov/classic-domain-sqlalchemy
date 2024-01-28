from typing import Sequence

from classic.components import component

from classic.criteria import Criteria, CompositeCriteria, And, Or, Invert, BoundCriteria
from classic.persistence.core import Repo as BaseRepo
from classic.persistence.core.repo import Entity

from sqlalchemy import select, Select, and_, or_, not_
from sqlalchemy.orm import Session


@component
class Repo(BaseRepo):
    session: Session

    def save(self, entity: Entity):
        self.session.add(entity)
        self.session.flush()

    def remove(self, instance):
        self.session.delete(instance)

    def get(self, id_) -> Entity:
        return self.session.get(self.entity_cls, id_)

    def find(self, criteria: Criteria, *options: Criteria) -> Sequence[Entity]:
        query = select(self.entity_cls)
        query, condition = self._criteria_to_query(query, criteria)
        query = query.where(condition)
        return self.session.execute(query).scalars().all()

    def _criteria_to_query(self, query: Select, criteria: Criteria):
        if isinstance(criteria, CompositeCriteria):
            conditions = []
            for criteria_ in criteria.criteria:
                query, condition = self._criteria_to_query(query, criteria_)
                condition.append(condition)

            if isinstance(criteria, And):
                func = and_
            elif isinstance(criteria, Or):
                func = or_
            else:
                raise NotImplemented

            return query, func(*conditions)

        elif isinstance(criteria, Invert):
            query, condition = self._criteria_to_query(query, criteria.criteria)
            return query, not_(condition)

        elif isinstance(criteria, BoundCriteria):
            try:
                translator = self.map[criteria.fn]
            except KeyError:
                raise ValueError(f'Для критерия {criteria} не найден метод-переводчик в {self}')

            return translator(query, criteria)
        else:
            raise NotImplemented
