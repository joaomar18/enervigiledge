###########EXTERNAL IMPORTS############

from dataclasses import dataclass, field
from typing import List, Optional

#######################################

#############LOCAL IMPORTS#############

#######################################


@dataclass
class QueryVariableLogs:
    variable: str
    fields: List[str] = field(default_factory=list)
    where: List[str] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    fill: Optional[str] = None
    timezone: Optional[str] = None

    def render(self) -> str:
        select = ", ".join(self.fields) if self.fields else "*"
        q = [f"SELECT {select}", f'FROM "{self.variable}"']
        if self.where:
            q.append(f"WHERE {' AND '.join(self.where)}")
        if self.group_by:
            q.append(f"GROUP BY {', '.join(self.group_by)}")
        if self.fill is not None:
            q.append(f"FILL({self.fill})")
        if self.timezone is not None:
            q.append(f"tz('{self.timezone}')")
        return " ".join(q)
